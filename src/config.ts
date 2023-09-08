import {Config as LakefsConfig, getConfigFromEnv} from "@quannadev/lakefs-ts";
import dotenv from 'dotenv'
import dotenvExpand from 'dotenv-expand'

export interface Config {
    lakefs: LakefsConfig;
    filePrefix: string;
    tableName: string;
    max_size: number;
    intervalTime: number;
}

export function fromEnvOrDefault(): Config {
    const envs = dotenv.config();
    dotenvExpand.expand(envs);

    const lakefs = {
        host: process.env.LAKEFS_HOST || "https://lakefs.quanna.dev",
        accessKeyId: process.env.LAKEFS_ACCESS_KEY_ID || "lakefs_root",
        secretAccessKey: process.env.LAKEFS_SECRET_ACCESS_KEY || "lakefs_root",
        repository: process.env.LAKEFS_REPOSITORY || "ethereum-test",
        branch: process.env.LAKEFS_BRANCH || "main",
    }

    return {
        lakefs,
        filePrefix: process.env.FILE_PREFIX || "ranges",
        tableName: process.env.TABLE_NAME || "blocks",
        max_size: parseInt(process.env.MAX_SIZE || "256"),
        intervalTime: parseInt(process.env.INTERVAL_TIME || "5"),
    }
}