const fs = require('fs');
const axios = require('axios');
const logging = require('console');
const BLOCK_SIZE = 1024;

async function get(masterUrl, file) {
    try {
        const fileTable = await axios.get(`${masterUrl}/read/${file}`);
        if (!fileTable.data || fileTable.data.length === 0) {
            logging.info("file not found");
            return;
        }

        for (let block of fileTable.data) {
            for (let { host, port } of block.block_addr) {
                try {
                    const response = await axios.get(`http://${host}:${port}/get/${block.block_id}`);
                    if (response.data) {
                        process.stdout.write(response.data);
                        break;
                    }
                } catch (e) {
                    continue;
                }
            }
        }
    } catch (error) {
        logging.error("No blocks found. Possibly a corrupt file");
    }
}

async function put(masterUrl, source, dest) {
    try {
        console.log(1)
        const size = fs.statSync(source).size;
        const response = await axios.post(`${masterUrl}/write`, { file: dest, size: size });
        const blocks = response.data;
        console.log(2)
        const fileStream = fs.createReadStream(source, { highWaterMark: BLOCK_SIZE });

        for (let block of blocks) {
            const data = await new Promise(resolve => {
                fileStream.once('data', resolve);
            });

            let { block_id, block_addr } = block;
            let addr = block_addr[0];
            let {host, port} = addr;
            block_addr.shift();
            console.log(block_addr);
            console.log(data.toString());
            await axios.post(`http://${host}:${port}/put`, {
                block_id: block_id,
                data: data.toString(),
                minions: block_addr
            });
        }
    } catch (error) {
        console.log(error.message);
    }
}

async function main(args) {
    const masterUrl = 'http://localhost:2131';
    console.log(masterUrl)
    if (args[0] === "get") {
        await get(masterUrl, args[1]);
    } else if (args[0] === "put") {
        await put(masterUrl, args[1], args[2]);
    } else {
        logging.error("try 'put srcFile destFile OR get file'");
    }
}

if (require.main === module) {
    main(process.argv.slice(2));
}
