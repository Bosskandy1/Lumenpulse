import {
  Injectable,
  Logger,
  OnModuleDestroy,
  BeforeApplicationShutdown,
} from '@nestjs/common';
import { SchedulerRegistry } from '@nestjs/schedule';
import { config } from '../lib/config';

@Injectable()
export class ShutdownService
  implements OnModuleDestroy, BeforeApplicationShutdown
{
  private readonly logger = new Logger(ShutdownService.name);
  private shuttingDown = false;

  constructor(private readonly schedulerRegistry: SchedulerRegistry) {}

  public isShuttingDown(): boolean {
    return this.shuttingDown;
  }

  onModuleDestroy() {
    this.logger.log('Application is destroying modules. Stopping schedulers...');
    try {
      const cronJobs = this.schedulerRegistry.getCronJobs();
      cronJobs.forEach((job, name) => {
        this.logger.log(`Stopping cron job: ${name}`);
        job.stop();
      });
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.warn(`Failed to stop cron jobs: ${errorMessage}`);
    }
  }

  async beforeApplicationShutdown(signal?: string) {
    this.logger.log(
      `Received ${signal}. Starting graceful shutdown sequence...`,
    );
    this.shuttingDown = true;

    const gracePeriodMs = config.SHUTDOWN_GRACE_PERIOD_MS;
    if (gracePeriodMs > 0) {
      this.logger.log(
        `Readiness probe is now unready. Waiting ${gracePeriodMs}ms for inflight requests to drain...`,
      );
      await new Promise((resolve) => setTimeout(resolve, gracePeriodMs));
      this.logger.log('Drain period completed. Proceeding to close HTTP server and database connections.');
    }
  }
}
