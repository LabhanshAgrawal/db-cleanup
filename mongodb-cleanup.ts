import {Filter, MongoClient, Sort} from 'mongodb';
import collectionPromise from './mongodb-client';

export type doc = {ts: number; version: string; platform: string; count: number};
export const cleanupDB = async (
  groupingPeriod: number,
  fetchingPeriod: number,
  sort: Sort
): Promise<[boolean, MongoClient, Partial<{start:string,end:string,groupingPeriod:string}>]> => {
  console.log('start cleanup');
  const client = await collectionPromise;
  const collection = client.db('hyper').collection<doc>('log');
  console.log('Connected to DB');
  const data = await collection.aggregate<{_id:number,ts:number}>([
    {
      $match: {
        $expr: {
          $and: [
            {$ne: [{$mod: ['$ts', groupingPeriod]}, 0]},
            {$lt: ['$ts', new Date().getTime() - Math.max(3 * groupingPeriod, fetchingPeriod)]},
          ],
        },
      },
    },
    {
      $group: {
        _id: {$subtract: ['$ts', {$mod: ['$ts', groupingPeriod]}]},
        ts: {$min: '$ts'},
      },
    },
    {
      $sort: sort,
    },
    {
      $limit: 1,
    },
  ]).toArray();

  if (data.length === 0) {
    console.log('No data');
    return [false, client, {}];
  } else {
    console.log('Fetched data');
  }

  const startTS = data[0]._id;
  let endTS = (data[0].ts + fetchingPeriod) - ((data[0].ts + fetchingPeriod) % fetchingPeriod);
  endTS = endTS > startTS + groupingPeriod ? endTS - (endTS % groupingPeriod) : endTS;

  console.log(
    'processing',
    new Date(startTS).toLocaleString('en-GB', {timeZone: 'Asia/Kolkata'}),
    'to',
    new Date(endTS).toLocaleString('en-GB', {timeZone: 'Asia/Kolkata'})
  );

  const dataFilter: Filter<doc> = {$expr: {$and: [{$gte: ['$ts', startTS]}, {$lt: ['$ts', endTS]}]}}

  const data2 = (
    await collection
      .find(dataFilter)
      .toArray()
  ).map((d) => ({version: d.version, platform: d.platform, count: d.count || 1, ts: d.ts}));

  const acc: Record<string,doc> = {};
  for (const d of data2) {
    const _ts = d.ts - (d.ts % groupingPeriod);
    const key = `${d.version}-${d.platform}-${_ts}`;
    if (acc[key]) {
      acc[key].count += d.count;
    } else {
      acc[key] = {...d, ts: _ts};
    }
  }
  const data3 = Object.values(acc);

  console.log('starting deletion');

  console.log(
    await collection.deleteMany(dataFilter)
  );

  console.log(
    await collection.insertMany(data3).then((r) => {
      const {insertedIds, ...x} = r;
      return x;
    })
  );
  console.log('processed');
  return [
    true,
    client,
    {
      start: new Date(startTS).toLocaleString('en-GB', {timeZone: 'Asia/Kolkata'}),
      end: new Date(endTS).toLocaleString('en-GB', {timeZone: 'Asia/Kolkata'}),
      groupingPeriod: `${groupingPeriod/(60*1000)} minutes`,
    }
  ];
  // return await cleanupDB(groupingPeriod, fetchingPeriod, sort);
};

if (require.main === module) {
  cleanupDB(60 * 60 * 1000, 24 * 60 * 60 * 1000, {ts: 1})
    .then(([success, client, result]) =>
      success
        ? ([success, client, result] as [boolean, MongoClient, Partial<{start:string,end:string,groupingPeriod:string}>])
        : cleanupDB(60 * 1000, 60 * 60 * 1000, {ts: 1})
    )
    .then(([success, client, result]) => client.close());
}

