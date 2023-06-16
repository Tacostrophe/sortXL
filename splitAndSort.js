import { once } from 'events';
import { lstatSync, createReadStream, createWriteStream } from 'fs';
import { createInterface } from 'readline';

async function splitAndSort(inputFileName, chunkSize) {
  const tmpFiles = [];
  const lines = [];
  let partOfLine = '';
  let reader, writer;
  const fileSize = lstatSync(inputFileName).size;
  const tmpDir = `files/tmp`;
  const tmpName = (num) => (`${tmpDir}/tmp_${num}.txt`);

  for (let i = 0; (i * chunkSize) < fileSize; i++) {
    reader = createInterface({
      input: createReadStream(inputFileName, {
        start: i * chunkSize,
        end: (i + 1) * chunkSize - 1,
        encoding: 'utf-8',
      }),
      crlfDelay: Infinity,
    });

    reader.on('line', (line) => {
      lines.push(line);
    });

    await once(reader, 'close');

    lines[0] = partOfLine + lines[0];
    if (((i + 1) * chunkSize) < fileSize) {
      partOfLine = lines.pop();
    }

    lines.sort();

    writer = createWriteStream(tmpName(i));
    tmpFiles.push(tmpName(i));
    writer.write(lines.join('\n'));
    lines.length = 0;
  }
  return tmpFiles;
}

export default splitAndSort;