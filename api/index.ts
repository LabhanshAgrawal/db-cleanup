import { VercelRequest, VercelResponse } from '@vercel/node'
import { cleanupDB } from '../mongodb-cleanup';

// Main function export
export default async (req: VercelRequest, res: VercelResponse) => {
  let [success, , result] = await cleanupDB(30 * 60 * 1000, 5 * 60 * 1000, {ts: 'desc'})

  if (!success) {
    [,,result] = await cleanupDB(60 * 1000, 5 * 60 * 1000, {ts: 'asc'})
  }

  res.json(result);

  return;
}
