import { lstatSync, createWriteStream, createReadStream } from 'fs';
import { createInterface } from 'readline';
import { once } from 'events';

async function mergeFiles(chunkSize ,pathToFile1, pathToFile2, pathToMerged) {
  // console.log('lets rock');
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
    // console.log('entering cycle');
    if (lines1Index >= lines1.length) {
      lines1.length = 0;
      lines1Index = 0;
      // console.log(`creating reader #${i1+1} for ${pathToFile1}`);
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
      // console.log(`stopped reading #${i1} for ${pathToFile1}`);

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
      // console.log(`creating reader #${i2+1} for ${pathToFile2}`);
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
      // console.log(`stopped reading #${i2} for ${pathToFile2}`);

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

  
  // console.log(lines1[0]);
  // console.log(lines1[lines1.length-1]);
  // console.log(`line1 length ${lines1.length}`);
  // console.log(`index1 - ${lines1Index}`);
  // console.log(partOfLine1);
  // console.log(`${chunkSize*i1} - ${fileSize1}`);
  // console.log('__________');
  // console.log(lines2[0]);
  // console.log(lines2[lines2.length-1]);
  // console.log(`line2 length ${lines2.length}`);
  // console.log(`index2 - ${lines2Index}`);
  // console.log(partOfLine2);
  // console.log(`${chunkSize*i2} - ${fileSize2}`);
  
  
  if(lines1Index < lines1.length) {
    // console.log(`${pathToFile2} ended`);
    lines1.push(partOfLine1);
    writer.write(lines1.slice(lines1Index).join('\n'));
  }

  if(lines2Index < lines2.length) {
    // console.log(`${pathToFile1} ended`);
    lines2.push(partOfLine2);
    writer.write(lines2.slice(lines2Index).join('\n'));
  }

  if((i1 * chunkSize) < fileSize1){
    // console.log(`piping ${pathToFile1} to ${pathToMerged}`);
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
    // console.log(`piping ${pathToFile2} to ${pathToMerged}`);
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
  // console.log('merged');
}

// (async function() {
//   await mergeFiles(20*1024*1024, 'files/tmp/tmp_0.txt', 'files/tmp/tmp_1.txt', 'files/tmp/tmpsum_0.txt');
//   const mbUsed = process.memoryUsage().heapUsed / 1024 / 1024;
//   console.log(`Used ${Math.round(mbUsed * 100) / 100} Mb`);
// })();

export default mergeFiles;