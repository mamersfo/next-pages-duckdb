import * as fs from 'fs'
import { Inter } from 'next/font/google'
import { getDuckDb, runQuery } from '@/lib/duckdb'

const inter = Inter({ subsets: ['latin'] })

type Measurement = { date: Date; value: number }

export async function loadParquet<T>(path: string): Promise<T[]> {
  const db = await getDuckDb()
  return runQuery(db, `select * from read_parquet('${path}')`)
}

export async function getStaticProps() {
  const parquetPath = 'public/distance.parquet'
  const data = await loadParquet<Measurement>(parquetPath)
  return { props: { data } }
}

export default function Home({ data }: { data: Measurement[] }) {
  return (
    <main
      className={`flex min-h-screen flex-col items-center justify-between p-24 ${inter.className}`}
    >
      <div className='flex flex-col'>
        <div>Measurements:</div>
        <pre className='text-sm'>{JSON.stringify(data, null, 2)}</pre>
      </div>
    </main>
  )
}
