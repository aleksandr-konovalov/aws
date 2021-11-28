const readStrem = process.stdin;
const writeStream = process.stdout;

readStrem
    .on('data', (chunk) => {
        writeStream.write(chunk.toString().split('').reverse().join(''));
    })
    .on('end', () => {
        writeStream.end();
    })
    .on('error', (e) => {
        console.error(e);
    });
