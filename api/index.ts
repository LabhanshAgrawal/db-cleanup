import { VercelRequest, VercelResponse } from '@vercel/node'
import { cleanupDB } from '../mongodb-cleanup';

// Main function export
export default async (req: VercelRequest, res: VercelResponse) => {
  let [success, , result] = await cleanupDB(30 * 60 * 1000, 10 * 60 * 1000, {ts: -1});

  if (!success) {
    [,,result] = await cleanupDB(1 * 60 * 1000, 10 * 60 * 1000, {ts: 1});
  }

  res.json(result);

  return;
}
