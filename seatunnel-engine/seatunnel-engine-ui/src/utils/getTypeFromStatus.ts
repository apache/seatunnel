import type { Job } from '@/service/job/types'

export const getTypeFromStatus = (status: Job['jobStatus']) => {
  switch (status) {
    case 'RUNNING':
      return 'success'
    case 'FINISHED':
      return 'error'
    default:
      return undefined
  }
}
