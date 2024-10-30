const express = require('express');
const fs = require('fs');
const path = require('path');
const zlib = require('zlib');
const app = express();
app.use(express.json());

const DATA_DIR = '/DistribtedFS/minion';
if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR);
}
app.post('/put', (req, res) => {
    const { block_id, data, minions } = req.body;
    const filePath = path.join(DATA_DIR, block_id);
    zlib.gzip(data, (err, compressedData) => {
        if (err) {
            return res.status(500).json({ error: 'Failed to compress data' });
        }
        fs.writeFileSync(filePath, compressedData);
    
        if (minions.length > 0) {
            forward(block_id, compressedData, minions);
        }
    
        res.json({ status: 'success' });
    });
});

app.get('/get/:block_id', (req, res) => {
    const block_id = req.params.block_id;
    const filePath = path.join(DATA_DIR, block_id);
    if (fs.existsSync(filePath)) {
        const compressedData = fs.readFileSync(filePath);

        // Decompress data before sending it to the client
        zlib.gunzip(compressedData, (err, data) => {
            if (err) {
                return res.status(500).send('Failed to decompress data');
            }
            res.send(data.toString());
        });
    } else {
        res.status(404).send('Block not found');
    }
});

function forward(block_id, data, minions) {
    const next_minion = minions.shift();
    const url = `http://${next_minion.host}:${next_minion.port}/put`;

    const axios = require('axios');
    axios.post(url, {
        block_id: block_id,
        data: data,
        minions: minions
    }).catch(error => {
        console.error(`Failed to forward block: ${error.message}`);
    });
}

const PORT = process.argv[2] || 3001;
app.listen(PORT, () => {
    console.log(`Minion server listening on port ${PORT}`);
});
