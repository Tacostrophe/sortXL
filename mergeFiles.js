import { lstatSync, createWriteStream, createReadStream } from 'fs';
import { createInterface } from 'readline';
import { once } from 'events';

async function mergeFiles(chunkSize ,pathToFile1, pathToFile2, pathToMerged) {
  const fileSize1 = lstatSync(pathToFile1).size;
  const fileSize2 = lstatSync(pathToFile2).size;
  let i1 = 0;
  let i2 = 0;
  let lines1Index = 0;
  let lines2Index = 0;
  const lines1 = [];
  const lines2 = [];
  let partOfLine1 = '';
  let partOfLine2 = '';
  let reader1, reader2;
  let writer = createWriteStream(pathToMerged);

  while (
    (((i1 * chunkSize) < fileSize1) || (lines1Index < lines1.length)) &&
    (((i2 * chunkSize) < fileSize2) || (lines2Index < lines2.length)) 
  ) {
    if (lines1Index >= lines1.length) {
      lines1.length = 0;
      lines1Index = 0;
      reader1 = createInterface({
        input: createReadStream(pathToFile1, {
          start: i1 * chunkSize,
          end: (++i1) * chunkSize - 1,
          encoding: 'utf-8',
        })
      });

      reader1.on('line', (line) => {
        lines1.push(line);
      });
      
      await once(reader1, 'close');

      lines1[0] = partOfLine1 + lines1[0];
      if ((i1) * chunkSize < fileSize1) {
        partOfLine1 = lines1.pop();
      } else {
        partOfLine1 = '';
      }
    }


    if (lines2Index >= lines2.length) {
      lines2.length = 0;
      lines2Index = 0;
      reader2 = createInterface({
        input: createReadStream(pathToFile2, {
          start: i2 * chunkSize,
          end: (++i2) * chunkSize - 1,
          encoding: 'utf-8',
        })
      });

      reader2.on('line', (line) => {
        lines2.push(line);  
      });
      
      await once(reader2, 'close');

      lines2[0] = partOfLine2 + lines2[0];
      if ((i2) * chunkSize < fileSize2) {
        partOfLine2 = lines2.pop();
      } else {
        partOfLine2 = '';
      }
    }

    while(lines1Index < lines1.length && lines2Index < lines2.length) {
      if (lines1[lines1Index] < lines2[lines2Index]) {
        if (!writer.write(lines1[lines1Index] + '\n')) {
          await once(writer, 'drain');
        }
        lines1Index++;
      } else {
        if (!writer.write(lines2[lines2Index] + '\n')) {
          await once(writer, 'drain');
        }
        lines2Index++;
      }
    }
  }
  
  if(lines1Index < lines1.length) {
    lines1.push(partOfLine1);
    writer.write(lines1.slice(lines1Index).join('\n'));
  }

  if(lines2Index < lines2.length) {
    lines2.push(partOfLine2);
    writer.write(lines2.slice(lines2Index).join('\n'));
  }

  if((i1 * chunkSize) < fileSize1){
    reader1 = createReadStream(pathToFile1, {
      start: i1 * chunkSize,
      encoding: 'utf-8',
    });
    
    reader1.pipe(writer);

    reader1.on('end', () => {
      lines1.length = 0;
      lines2.length = 0;
    })
  } else if((i2 * chunkSize) < fileSize2){
    reader2 = createReadStream(pathToFile2, {
      start: i2 * chunkSize,
      encoding: 'utf-8',
    });
    
    reader2.pipe(writer);

    reader1.on('end', () => {
      lines1.length = 0;
      lines2.length = 0;
    })
  }
}

export default mergeFiles;