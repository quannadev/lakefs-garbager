import {ObjectItem, ObjectsService, QueryParams} from "@quannadev/lakefs-ts";
import {Connection, Database} from "duckdb-async";
import {Config} from "../config";

export class Garbage {
    config: Config;
    conn: Connection;
    tableName: string = "blocks";
    filePrefix: string = "range";

    constructor(config: Config) {
        this.config = config;
        this.tableName = config.tableName;
        this.filePrefix = config.filePrefix;

        Database.create(':memory:')
            .then(async (db) => {
                this.conn = await db.connect();
            })
            .catch(err => {
                console.error(err);
                process.exit(1);
            })
    }

    async setupS3(): Promise<boolean> {
        const {host, secretAccessKey, accessKeyId} = this.config.lakefs
        const regex = /http?s:\/\//g;
        const host_regex = host.replace(regex, "");
        const isHttps = host.includes("https");
        let query_array = [
            "INSTALL httpfs;",
            "LOAD httpfs;",
            `SET s3_endpoint=`.concat(`'${host_regex}';`),
            `SET s3_region='us-east-1';`,
            `SET s3_use_ssl=${isHttps};`,
            `SET s3_url_style='path';`,
            `SET s3_access_key_id='${accessKeyId}';`,
            `SET s3_secret_access_key='${secretAccessKey}';`
        ]
        for (let q of query_array) {
            const smt = await this.conn.prepare(q);
            await smt.run();
            await smt.finalize();
        }
        return true;
    }

    getS3Url(ojs: ObjectItem): string {
        const {repository, branch} = this.config.lakefs;
        return `s3://${repository}/${branch}/${ojs.path}`
    }

    async createTableFromS3(s3path: string | null): Promise<boolean> {
        if (!s3path) {
            const objectFiles = new ObjectsService(this.config.lakefs);
            const objects = await objectFiles.getObjects({
                amount: 1,
            });
            s3path = this.getS3Url(objects.results[0]);
        }
        const smt = await this.conn.prepare(`CREATE TABLE IF NOT EXISTS ${this.tableName} AS
        SELECT *
        FROM read_parquet('${s3path}');`);
        await smt.run();
        await smt.finalize();
        return true;
    }

    async insertFileToTable(obj: ObjectItem): Promise<boolean> {
        const smt = await this.conn.prepare(`INSERT INTO ${this.tableName}
                                             SELECT *
                                             FROM read_parquet('${obj.physical_address}');`);
        await smt.run();
        await smt.finalize();
        return true;
    }

    async getRangeName() {
        const query = `SELECT MIN(block_number) as min_block_number,
                              MAX(block_number) as max_block_number
                       FROM ${this.tableName};`
        const smt = await this.conn.prepare(query);
        const rows = await smt.all();
        const firstRow = rows[0];
        const min_block_number: number = firstRow.min_block_number;
        const max_block_number: number = firstRow.max_block_number;
        await smt.finalize();
        return `${this.filePrefix}__${min_block_number}-${max_block_number}.parquet`;
    }

    async copyTableToS3() {
        const file_name = await this.getRangeName();
        const {repository, branch} = this.config.lakefs;
        const s3path = `s3://${repository}/${branch}/${file_name}`;
        const smt = await this.conn.prepare(`COPY ${this.tableName} TO '${s3path}' (FORMAT 'PARQUET', CODEC 'ZSTD');`);
        await smt.run();
        await smt.finalize();
    }

    async truncateTable() {
        const smt = await this.conn.prepare(`DELETE
                                             FROM ${this.tableName};`);
        await smt.run();
        await smt.finalize();
        return true;
    }

    async exportTableToFile() {
        const file_name = await this.getRangeName();
        const smt = await this.conn.prepare(`COPY ${this.tableName} TO './data/${file_name}' (FORMAT 'PARQUET', CODEC 'ZSTD');`);
        await smt.run();
        await smt.finalize();
    }

    async sleep(ms: number) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async run() {
        const objectFiles = new ObjectsService(this.config.lakefs);
        let query_params: QueryParams = {
            amount: 100,
            after: ''
        }
        let files = Array<ObjectItem>();
        let total_size = 0;
        while (true) {
            if (files.length === 0) {
                const result = await objectFiles.getObjects(query_params);
                if (result.pagination.has_more) {
                    query_params.after = result.pagination.next_offset;
                }
                files = result.results;
            }
            const file = files.shift();
            if (!file) {
                await this.sleep(this.config.intervalTime * 1000)
                continue;
            }
            if (file.size_bytes >= this.config.max_size * 1024 * 1024) {
                //ignore file
                continue;
            }
            if (await this.insertFileToTable(file)) {
                total_size += file.size_bytes;
                if (total_size >= this.config.max_size * 1024 * 1024) {
                    // await this.copyTableToS3();
                    await this.exportTableToFile();
                    await this.truncateTable();
                    total_size = 0;
                }
            }
            await this.sleep(this.config.intervalTime * 1000)
        }
    }
}