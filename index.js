import * as events from 'events';
import * as fs from 'fs';
import * as readline from 'readline';

const args = process.argv;
const fileDir = 'files';
const inputFileName = args[2] ?? (fileDir + '/input.txt');
const outputFileName = args[3] ?? (fileDir + '/output.txt');
const tmpDir = `${fileDir}/tmp`;
const tmpName = (num) => (`${tmpDir}/tmp_${num}.txt`);
const tmpFiles = [];
const getFileSize = (path) => fs.lstatSync(path).size;
const chunkSize = 64 * 1024 * 1024;

async function splitAndSort() {
  const lines = [];
  let partOfLine = '';
  let reader, writer;
  const fileSize = getFileSize(inputFileName);
  for (let i = 0; (i * chunkSize) < fileSize; i++) {
    reader = readline.createInterface({
      input: fs.createReadStream(inputFileName, {
        start: i * chunkSize,
        end: (i + 1) * chunkSize - 1,
        encoding: 'utf-8',
      }),
      crlfDelay: Infinity,
    });

    reader.on('line', (line) => {
      lines.push(line);
    });

    await events.once(reader, 'close');

    lines[0] = partOfLine + lines[0];
    if (((i + 1) * chunkSize) < fileSize) {
      partOfLine = lines.pop();
    }

    lines.sort();

    writer = fs.createWriteStream(tmpName(i));
    tmpFiles.push(tmpName(i));
    writer.write(lines.join('\n'));
    lines.length = 0;

  }
}

async function mergeFiles() {
  
  let firstFileSize, firstReader, firstFileName;
  let secondFileSize, secondReader, secondFileName;
  let tempFile;
  let i = 0;
  let firstCurrChunk = 0;
  let secondCurrChunk = 0;
  const firstLines = [];
  const secondLines = [];
  const sortedLines = [];
  let writer;
  const createTmpSumName = (num) => `${tmpDir}/tmpsum_${num}.txt`;
  let tmpSumName;
  let tmpSumCounter = 0;
  let firstPartOfLine = '';
  let secondPartOfLine = '';
  while (true) {
    if (tmpFiles.length === 1) {
      fs.rename(tmpFiles[0], outputFileName, function (err) {
        if (err) throw err;
      });
      return;
    }

    firstFileName = tmpFiles[i];
    secondFileName = tmpFiles[i+1]
    tmpSumName = createTmpSumName(tmpSumCounter++);
    console.log(`trying ${firstFileName}+${secondFileName}=>${tmpSumName}`)
    writer = fs.createWriteStream(tmpSumName, {flags: 'a'});
    firstFileSize = getFileSize(firstFileName);
    secondFileSize = getFileSize(secondFileName);

    while (
      (firstCurrChunk * chunkSize / 2) < firstFileSize ||
      (secondCurrChunk * chunkSize / 2) < secondFileSize
      ) {

      if (firstLines.length === 0) {
        console.log(`creating firstReader for chunk ${firstCurrChunk}`);
        firstReader = readline.createInterface({
          input: fs.createReadStream(firstFileName, {
            start: firstCurrChunk * chunkSize / 2,
            end: (++firstCurrChunk) * chunkSize / 2 - 1,
            encoding: 'utf-8',
          }),
          crlfDelay: Infinity,
        });
        firstReader.on('line', (line) => {
          firstLines.push(line);
        });
        await events.once(firstReader, 'close');

        firstLines[0] = firstPartOfLine + firstLines[0];
        if (((firstCurrChunk + 1) * chunkSize) < firstFileSize) {
          firstPartOfLine = firstLines.pop();
        }

        console.log(`stopped reading chunk ${firstCurrChunk-1} of ${firstFileName}`);
      }

      if (secondLines.length === 0) {
        console.log(`creating secondReader for chunk ${secondCurrChunk}`);
        secondReader = readline.createInterface({
          input: fs.createReadStream(secondFileName, {
            start: secondCurrChunk * chunkSize / 2,
            end: (++secondCurrChunk) * chunkSize / 2 - 1,
            encoding: 'utf-8',
          }),
          crlfDelay: Infinity,
        });
        secondReader.on('line', (line) => {
          secondLines.push(line);
        });
        await events.once(secondReader, 'close');

        secondLines[0] = secondPartOfLine + secondLines[0];
        if (((secondCurrChunk + 1) * chunkSize) < secondFileSize) {
          secondPartOfLine = secondLines.pop();
        }
        console.log(`stopped reading chunk ${secondCurrChunk-1} of ${secondFileName}`);
      }

    
      try {
        if (firstLines[0] > secondLines[0]) {
          if (!writer.write(secondLines.shift() + '\n')) {
            await events.once(writer, 'drain');
          }
        } else {
          if (!writer.write(firstLines.shift() + '\n')) {
            await events.once(writer, 'drain');
          }
        }
      } catch (err) {
        console.log(err);
        console.log(firstLines[0]);
        console.log(secodLines[0]);
      }
    }

    if (firstLines.length > 0) {
      console.log('first arr left, write it into sum')
      writer.write(firstLines.join('\n'));
      firstLines.length = 0;
    } else {
      console.log('second arr left, write it into sum')
      writer.write(secondLines.join('\n'));
    }

    if ( (firstCurrChunk * chunkSize / 2) < firstFileSize ) {
      console.log('first file left, write it into sum')
      firstReader = readline.createInterface({
        input: fs.createReadStream(firstFileName, {
          start: firstCurrChunk * chunkSize / 2,
          encoding: 'utf-8',
        }),
        crlfDelay: Infinity,
      });
      firstReader.on('line', async (line) => {
        if (!writer.write(line + '\n')) {
          await events.once(writer, 'drain');
        }
      });
    }

    if ( (secondCurrChunk * chunkSize / 2) < secondFileSize) {
      console.log('second file left, write it into sum')
      secondReader = readline.createInterface({
        input: fs.createReadStream(secondFileName, {
          start: secondCurrChunk * chunkSize / 2,
          encoding: 'utf-8',
        }),
        crlfDelay: Infinity,
      });
      secondReader.on('line', async (line) => {
        if (!writer.write(line + '\n')) {
          await events.once(writer, 'drain');
        }
      });
    }
    await events.once(writer, 'finish');
    console.log('sum done');
    fs.unlink(tmpFiles[i]);
    fs.unlink(tmpFiles[i+1]);
    tmpFiles.splice(i, 2, tmpSumName);

    i = (i > (tmpFiles.length - 3)) ? 0 : (i + 2);
  }
}

(async function () {
  await splitAndSort();
  await mergeFiles();
  console.log(tmpFiles);
  // console.log(process.memoryUsage());
  const mbUsed = process.memoryUsage().heapUsed / 1024 / 1024;
  console.log(`Used ${Math.round(mbUsed * 100) / 100} Mb`);
})();
