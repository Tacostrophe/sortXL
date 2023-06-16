import { unlink, rename } from 'fs';
import mergeFiles from './mergeFiles.js';
import splitAndSort from './splitAndSort.js';

const args = process.argv;
const fileDir = 'files';
const inputFileName = args[2] ?? (fileDir + '/input.txt');
const outputFileName = args[3] ?? (fileDir + '/output.txt');
const chunkSize = 20 * 1024 * 1024;

(async function () {
  const tmpFiles = await splitAndSort(inputFileName, chunkSize);
  console.log(tmpFiles);
  let i = 0;
  let setPathToMerged = (index) => `files/tmp/tmp_merged_${index}.txt`;
  let pathToMerged;
  let mergedCounter = 0;
  while(tmpFiles.length > 1) {
    pathToMerged = setPathToMerged(mergedCounter++);
    console.log(i);
    console.log(`${tmpFiles[i]}+${tmpFiles[(i+1)]}=>${pathToMerged}`);
    await mergeFiles(chunkSize/2, tmpFiles[i], tmpFiles[(i+1)], pathToMerged);
    // unlink(tmpFiles[i], (err) => {if (err) throw (err);});
    // unlink(tmpFiles[i+1], (err) => {if (err) throw (err);});
    tmpFiles.splice(i, 2, pathToMerged);
    console.log(tmpFiles.length);
    i = (i > (tmpFiles.length - 2)) ? 0 : i+1;
    console.log(tmpFiles);
  }
  rename(tmpFiles[0], 'files/output.txt', (err) => {if (err) throw (err);});
  // await mergeFiles(tmpFiles[0], tmpFiles[1], 'files/tmp/tmpsum_0');
  // console.log(process.memoryUsage());
  const mbUsed = process.memoryUsage().heapUsed / 1024 / 1024;
  console.log(`Used ${Math.round(mbUsed * 100) / 100} Mb`);
})();
