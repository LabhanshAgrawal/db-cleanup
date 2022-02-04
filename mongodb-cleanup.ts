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
  const data = await collection.findOne(
    {
      $expr: {
        $and: [
          {$ne: [{$mod: ['$ts', groupingPeriod]}, 0]},
          {$lt: ['$ts', new Date().getTime() - Math.max(3 * groupingPeriod, fetchingPeriod)]},
        ],
      },
    },
    {
      sort,
    }
  );

  if (!data) {
    console.log('No data');
    return [false, client, {}];
  } else {
    console.log('Fetched data');
  }

  const baseTS = data.ts - (data.ts % groupingPeriod);

  const latestData = await collection.findOne(
    { $expr: { $and: [{$gt: ['$ts', baseTS]}, {$lt: ['$ts', baseTS + groupingPeriod]}], }, },
    { sort: { ts: 'asc', }, }
  );

  const latestTS = latestData!.ts;

  console.log(
    'processing',
    new Date(baseTS).toLocaleString('en-GB', {timeZone: 'Asia/Kolkata'}),
    'to',
    new Date(latestTS + fetchingPeriod).toLocaleString('en-GB', {timeZone: 'Asia/Kolkata'})
  );

  const dataFilter: Filter<doc> = {$expr: {$and: [{$gte: ['$ts', baseTS]}, {$lt: ['$ts', latestTS + fetchingPeriod]}]}}

  const data2 = (
    await collection
      .find(dataFilter)
      .toArray()
  ).map((d) => ({version: d.version, platform: d.platform, count: d.count || 1, ts: d.ts}));

  const data3 = Object.values(data2.reduce((acc, d) => {
    const _ts = d.ts - (d.ts % groupingPeriod);
    const key = `${d.version}-${d.platform}-${_ts}`;
    if (acc[key]) {
      acc[key].count += d.count;
    } else {
      acc[key] = {...d, ts: _ts};
    }
    return acc;
  }, {} as Record<string, doc>));

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
      start: new Date(baseTS).toLocaleString('en-GB', {timeZone: 'Asia/Kolkata'}),
      end: new Date(latestTS + fetchingPeriod).toLocaleString('en-GB', {timeZone: 'Asia/Kolkata'}),
      groupingPeriod: `${groupingPeriod/(60*1000)} minutes`,
    }
  ];
  // return await cleanupDB(groupingPeriod, fetchingPeriod, sort);
};

if (require.main === module) {
  cleanupDB(30 * 60 * 1000, 30 * 60 * 1000, {ts: 'desc'})
    .then(([success, client, result]) =>
      success
        ? ([success, client, result] as [boolean, MongoClient, Partial<{start:string,end:string,groupingPeriod:string}>])
        : cleanupDB(60 * 1000, 30 * 60 * 1000, {ts: 'asc'})
    )
    .then(([success, client, result]) => client.close());
}

