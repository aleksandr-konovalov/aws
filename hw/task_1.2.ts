import path from 'path';
import { createReadStream, createWriteStream } from 'fs';
import csvToJson from 'csvtojson/v2';
import { pipeline, Transform } from 'stream';

const inputFilePath = path.join(__dirname, 'csv', 'input.csv');
const outputFilePath = path.resolve(__dirname, 'csv', 'output.txt');

const inputStream = createReadStream(inputFilePath);
const outputStream = createWriteStream(outputFilePath);
const dbStream = new Transform({
    transform: function (this, chunk, _encoding, callback) {
        this.push(chunk);
        new Promise((resolve) => {
            setTimeout(resolve, 300);
        });
        callback();
    },
});

type BookInfo = {
    Book: string;
    Author: string;
    Amount: string;
    Price: string;
};
const transformStream = new Transform({
    transform: function (this, chunk, _encoding, callback) {
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

inputStream
    .on('error', (e) => {
        console.error(e);
    })
    .on('end', () => {
        outputStream.end();
        dbStream.end();
        transformStream.end();
    });

outputStream.on('error', (e) => {
    console.error(e);
});

dbStream.on('error', (e) => {
    console.error(e);
});

transformStream.on('error', (e) => {
    console.error(e);
});

pipeline(inputStream, csvToJson(), transformStream, dbStream, outputStream, (e) => {
    if (e) {
        console.error(e);
    }
});
