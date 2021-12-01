import path from 'path';
import { createReadStream, createWriteStream } from 'fs';
import csvToJson from 'csvtojson/v2';
import { Transform, pipeline } from 'stream';

const inputFilePath = path.join(__dirname, 'csv', 'input.csv');
const outputFilePath = path.resolve(__dirname, 'csv', 'output.txt');

const inputStream = createReadStream(inputFilePath);
const outputStream = createWriteStream(outputFilePath);
const dbStream = new Transform({
    transform: function (chunk, _encoding, callback) {
        setTimeout(() => {
            this.push(chunk);
            callback();
        }, 100);
    },
});

type BookInfo = {
    Book: string;
    Author: string;
    Amount: string;
    Price: string;
};
const transformStream = new Transform({
    transform: function (chunk, _encoding, callback) {
        try {
            const bookInfo: BookInfo = JSON.parse(chunk.toString());
            const transformedChunk = `${JSON.stringify({
                book: bookInfo.Book,
                author: bookInfo.Author,
                price: bookInfo.Price,
            })}\n`;
            this.push(transformedChunk);
            callback();
        } catch (e) {
            this.push(chunk);
            callback();
        }
    },
});

pipeline(inputStream, csvToJson(), transformStream, dbStream, outputStream, (e) => {
    if (e) {
        console.error('Pipeline failed', e);
    } else {
        console.log('Pipeline succeeded');
    }
});
