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
    // console.log(lines[0]);
    // console.log(lines[0].length);
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
  if (tmpFiles.length === 1) {
    fs.rename(tmpFiles[0], outputFileName, function (err) {
      if (err) throw err;
    });
    return;
  }

  let firstFileSize, firstReader, firstFileName;
  let secondFileSize, secondReader, secondFileName;
  let tempFile;
  const sortedArr = [];
  let i = 0;
  let jFirst = 0;
  let jSecond = 0;
  const firstArr = [];
  const secondArr = [];
  let writer;
  const tmpSumName = (num) => `${tmpDir}/tmpsum_${num}.txt`;
  const tmpSumCounter = 0;
  while (tmpFiles.length > 1) {
    firstFileName = tmpFiles[i];
    secondFileName = tmpFiles[i+1]
    writer = fs.createWriteStream(tmpSumName(tmpSumCounter++), {flags: 'a'});
    firstFileSize = getFileSize(firstFileName);
    secondFileSize = getFileSize(secondFileName);

    while (
      (jFirst * chunkSize / 2) < firstFileSize ||
      (jSecond * chunkSize / 2) < secondFileSize
      ) {
      firstReader = readline.createInterface({
        input: fs.createReadStream(firstFileName, {
          start: jFirst * chunkSize / 2,
          end: (jFirst + 1) * chunkSize / 2 - 1,
          encoding: 'utf-8',
        })
      });
      secondReader = readline.createInterface({
        input: fs.createReadStream(secondFileName, {
          start: jSecond * chunkSize /2,
          end: (jSecond + 1) * chunkSize / 2 - 1,
          encoding: 'utf-8',
        })
      })
      
    }
    i = (i > (tmpFiles.length - 3)) ? 0 : (i + 2);
  }
}

(async function () {
  await splitAndSort();
  console.log(tmpFiles);
  // console.log(process.memoryUsage());
  const mbUsed = process.memoryUsage().heapUsed / 1024 / 1024;
  console.log(`Used ${Math.round(mbUsed * 100) / 100} Mb`);
})();
