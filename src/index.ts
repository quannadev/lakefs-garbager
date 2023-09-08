import {Garbage} from "./garbager/core";
import {fromEnvOrDefault} from "./config";

async function main() {
    const config = fromEnvOrDefault();
    const garbage = new Garbage(config);
    const setup = await garbage.setupS3();
    if (!setup) {
        console.error("Failed to setup S3");
        process.exit(1);
    }
    const setupTable = await garbage.createTableFromS3(null);
    if (!setupTable) {
        console.error("Failed to setup table");
        process.exit(1);
    }
    await garbage.run();
}

main().then(() => {
    console.log("Done");
}).catch((err) => {
    console.error(err);
    process.exit(1);
});