import { MongoClient } from 'mongodb';
import collectionPromise from './mongodb-client';

export type doc = {ts: number; version: string; platform: string; count: number};

export const cleanupDB = async (groupingPeriod: number): Promise<MongoClient> => {
  console.log('Updating DB');
  const client = await collectionPromise;
  const collection = client.db('hyper').collection('log');
  console.log('Connected to DB');
  const data = await collection.findOne({
    $expr: {
      $and: [
        {
          $ne: [
            {
              $mod: ['$ts', groupingPeriod]
            },
            0
          ]
        },
        {
          $lt: [
            '$ts',
            new Date().getTime() - (3 * groupingPeriod)
          ]
        }
      ]
    }
  });

  if (!data) {
    console.log('No data');
    return client;
  } else {
    console.log('Fetched data');
  }

  const ts = data.ts - (data.ts % groupingPeriod);

  console.log('processing', new Date(ts).toLocaleString('en-GB', {timeZone: 'IST'}));

  const data2 = (
    await collection
      .find(
        {
          $expr: {
            $eq: [{$subtract: ['$ts', {$mod: ['$ts', groupingPeriod]}]}, ts]
          }
        }
      )
      .toArray()
  ).map((d) => ({version: d.version, platform: d.platform, count: d.count || 1}));

  const data3 = data2.reduce((acc, curr) => {
    const idx = acc.findIndex((d) => d.version === curr.version && d.platform === curr.platform);
    if (idx === -1) {
      acc.push({...curr, ts: ts});
    } else {
      acc[idx].count += curr.count;
    }
    return acc;
  }, [] as doc[]);

  console.log(
    await collection.deleteMany({
      $expr: {
        $eq: [{$subtract: ['$ts', {$mod: ['$ts', groupingPeriod]}]}, ts]
      }
    })
  );

  console.log(
    await collection.insertMany(data3).then((r) => {
      const {insertedIds, ...x} = r;
      return x;
    })
  );
  console.log('processed', new Date(ts).toLocaleString('en-GB', {timeZone: 'IST'}));
  return await cleanupDB(groupingPeriod);
};

cleanupDB(30 * 60 * 1000)
  .then(() => cleanupDB(60 * 1000))
  .then((client)=> client.close());