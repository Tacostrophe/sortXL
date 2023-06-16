import { mkdir, rmSync, rename, existsSync, mkdirSync } from 'fs';
import mergeFiles from './mergeFiles.js';
import splitAndSort from './splitAndSort.js';

const args = process.argv;
if (args.length < 3) {
  throw Error('Path to file is required');
}
const inputFileName = args[2];
const fileDir = inputFileName.slice(0, (inputFileName.lastIndexOf('/')==-1) ? '' : inputFileName.lastIndexOf('/'));
const tmpDir = `${fileDir}/tmp`;
const outputFileName = args[3] ?? (`${fileDir}/output.txt`);
const chunkSize = 16 * 1024 * 1024;

(async function () {
  if (!existsSync(tmpDir)) {
    mkdirSync(tmpDir, {recursive: true});
  }
  
  const tmpFiles = await splitAndSort(inputFileName, chunkSize, tmpDir);
  let i;
  let setPathToMerged = (index) => `${tmpDir}/tmp_merged_${index}.txt`;
  let pathToMerged;
  let mergedCounter = 0;
  const filesToDel = [];
  while(tmpFiles.length > 1) {
    i = 0;

    while(i < tmpFiles.length - 1) {
      pathToMerged = setPathToMerged(mergedCounter++);
      await mergeFiles(chunkSize/2, tmpFiles[i], tmpFiles[(i+1)], pathToMerged);
      filesToDel.push(tmpFiles[i], tmpFiles[i+1]);
      tmpFiles.splice(i, 2, pathToMerged);
      i++;
    }

  }

  rename(tmpFiles[0], outputFileName, (err) => {if (err) throw (err);});
  rmSync(tmpDir, {recursive: true});

  const mbUsed = process.memoryUsage().heapUsed / 1024 / 1024;
  console.log(`Used ${Math.round(mbUsed * 100) / 100} Mb`);
})();
