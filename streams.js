const fs = require('fs');
const { Transform, pipeline } = require('stream');
const { promisify } = require('util');





const pipelineAsync = promisify(pipeline); // через этот объект будем передавать данные из одного потока (чтение) в другой поток (запись)



async function indexText(inputFile, outputFile) {
    const readStream = fs.createReadStream(inputFile, { encoding: 'utf-8' });
    const writeStream = fs.createWriteStream(outputFile);

    const wordsCounts = new Map();

    // поток для обработки слов:
    const wordProcessor = new Transform({
        transform(chunk, encoding, callback) {
            const text = chunk.toString(); // берем по частям текст для индексации.
            
            const words = text
                .toLowerCase()
                .split(/[\s\n]+/) // Разделяем по пробелам и переводам строк
                .map(word => word.replace(/[^a-zа-яё]/g, '')) // Удаляем все не-буквы (т.к. текст - только буквы, по идее)
                .filter(word => word.length > 0);

            words.forEach(word => {
                wordsCounts.set(word, (wordsCounts.get(word) || 0) + 1);
            });

            callback();
        },
        flush(callback) { // метод, вызывающийся по завершении чтения потоком или перед его закрытием, НО до вызова callback в transform(args).
            
            const sortedWords = [...wordsCounts.keys()].sort(); // [...коллекция] - превращение в массив - аналог Array.from(collection);
            const resultVector = sortedWords.map(word => wordsCounts.get(word));

            this.push(JSON.stringify(resultVector, null, 2)); // this.push отправляет данные в след. поток в рамках Transform'а.

            callback();
        }
    });

    try {
        await pipelineAsync(
            readStream,
            wordProcessor,
            writeStream
        );
        console.log(`Текст проиндексирован. Результат: ${outputFile}`);
    } catch (err) {
        console.error('Exception: ', err);
    }
}



const [inputFile, outputFile = 'result.txt'] = process.argv.slice(2);



if (!inputFile) {
    console.error('Использование: node streams.js <inputFile> [outputFile]');
    process.exit();
}



indexText(inputFile, outputFile);