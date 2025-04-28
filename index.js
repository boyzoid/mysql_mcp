import {McpServer, ResourceTemplate} from "@modelcontextprotocol/sdk/server/mcp.js";
import {StdioServerTransport} from "@modelcontextprotocol/sdk/server/stdio.js";
import SqlParser from 'node-sql-parser';
import dotenv from 'dotenv'
import mysql from "mysql2/promise";
import {z} from "zod";

// Load environment variables
dotenv.config();


// Database connection configuration
const dbConfig = {
    host: process.env.MYSQL_DB_HOST || 'localhost',
    user: process.env.MYSQL_DB_USER || 'root',
    password: process.env.MYSQL_DB_PASSWORD || '',
    database: process.env.MYSQL_DB_NAME || 'mysql',
    port: process.env.MYSQL_DB_PORT ? parseInt(process.env.MYSQL_DB_PORT) : 3306,
    waitForConnections: true,
    connectionLimit: 10,
    maxIdle: 10,
    idleTimeout: 60000,
    queueLimit: 0,
    enableKeepAlive: true,
    keepAliveInitialDelay: 0
};


// Create a connection pool
const pool = await mysql.createPool(dbConfig)

const SCHEMA_PATH = "schema"

const parser = new SqlParser.Parser()

// Initialize the server
const server = new McpServer(
    {
        id: 'mysql_mcp_server',
        name: 'MySQL MCP Server',
        description: 'MySQL MCP server',
        version: '0.0.1'
    }
)

server.tool(
    "execute_query",
    {sql: z.string().describe("MySQL query to execute")},
    async ({sql}) => {
        try {
            return await executeQuery(sql)
        }
        catch (e) {
            console.error(e)
            return {
                content: [
                    {
                        type: 'text',
                        text: 'Error executing query',
                    },
                ],
                isError: true,
            };
        }
    }
)

const executeQuery = async (sql)=>{
    const queryTypes = await getQueryTypes(sql)
    const isMutatableQuery = queryTypes.some(type => ['insert', 'update', 'delete', 'drop', 'truncate', 'rename'].includes(type))
    if(isMutatableQuery){
        return {
            content: [
                {
                    type: 'text',
                    text: 'Error: Only SELECT, SHOW, or DESCRIBE statements are allowed.',
                },
            ],
            isError: true,
        };
    }
    else{
        const conn = await pool.getConnection()
        await conn.query('SET SESSION TRANSACTION READ ONLY')
        await conn.beginTransaction()
        try{
            const [rows] = await conn.query(sql)
            await conn.rollback()
            return {
                content: [
                    {
                        type: 'text',
                        text: JSON.stringify(rows, null, 2),
                    },
                ],
                isError: false,
            }
        }
        catch(e){
            await conn.rollback()
            return {
                content: [
                    {
                        type: 'text',
                        text: 'Error executing query',
                    },
                ],
                isError: true,
            }
        }
        finally {
            if(conn){
                conn.release()
            }
        }
    }
}

const getQueryTypes = async (sql) => {
    try{
        const ast = parser.astify(sql, {database: 'mysql'})
        const statements = Array.isArray(ast) ? ast : [ast]
        return statements.map(stmt => stmt.type?.toLowerCase() ?? 'unknown');
    }
    catch(e){
        throw new Error(`Parsing failed: ${e.message}`);
    }
}

async function runServer() {
    const transport = new StdioServerTransport();
    await server.connect(transport);
}

runServer().catch(console.error);