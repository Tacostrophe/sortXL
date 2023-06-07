import * as events from 'events';
import * as fs from 'fs';
import * as readline from 'readline';

const args = process.argv;
const fileName = args[2] ?? 'file.txt';

async function processLineByLine() {
  const rl = readline.createInterface({
    input: fs.createReadStream(fileName),
    crlfDelay: Infinity,
  });

  rl.on('line', (line) => {
    console.log(`line frome file: ${line}`);
  });

  await events.once(rl, 'close');

  const mbUsed = process.memoryUsage().heapUsed / 1024 / 1024;
  console.log(`Used ${Math.round(mbUsed * 100) / 100 mb}`);
}

processLineByLine();
