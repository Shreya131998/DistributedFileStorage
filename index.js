const express = require('express');
const app = express();
app.use(express.json());

const BLOCK_SIZE = 1024;
const fileTable = {}; // Simulates a file table in memory

app.get('/read/:file', (req, res) => {
    const file = req.params.file;
    console.log(file)
    console.log(fileTable)
    if (fileTable[file]) {
        res.json(fileTable[file]);
    } else {
        res.status(404).json([]);
    }
});

app.post('/write', (req, res) => {
    const { file, size } = req.body;
    console.log("master")
    const numBlocks = Math.ceil(size / BLOCK_SIZE);
    const blocks = [];

    for (let i = 0; i < numBlocks; i++) {
        const block_id = `block_${file}_${i}`;
        const block_addr = [
            { host: 'localhost', port: 3001 },
            { host: 'localhost', port: 3002 }
        ];
        blocks.push({ block_id, block_addr });
    }

    fileTable[file] = blocks;
    res.json(blocks);
});

const PORT = 2131;
app.listen(PORT, () => {
    console.log(`Master server listening on port ${PORT}`);
});
