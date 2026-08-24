import { Injectable, Logger } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { ConfigService } from '@nestjs/config';
import { firstValueFrom } from 'rxjs';
import { AxiosError } from 'axios';

export interface RetrainResult {
  status: string;
  started_at?: string;
  finished_at?: string;
  duration_seconds?: number;
  models?: Record<string, unknown>;
  registry?: Record<string, unknown>;
  error?: string;
}

export interface ModelStatusResult {
  last_run: Record<string, unknown>;
  registry: Record<string, unknown>;
}

/**
 * Response returned by the Python service when a long-running operation is
 * accepted onto the async job queue (HTTP 202).
 */
export interface JobSubmissionResponse {
  job_id: string;
  status: string;
  status_url: string;
}

/**
 * Response returned by the Python service's job-status endpoint
 * (`GET /jobs/{job_id}`).
 */
export interface JobStatusResponse<T = unknown> {
  job_id: string;
  type: string;
  status: 'queued' | 'running' | 'succeeded' | 'failed';
  result_ref?: string | null;
  result?: T | null;
  error?: string | null;
  created_at: string;
  updated_at: string;
}

const TERMINAL_JOB_STATES = ['succeeded', 'failed'];

@Injectable()
export class ModelRetrainingService {
  private readonly logger = new Logger(ModelRetrainingService.name);
  private readonly pythonApiUrl: string;
  private readonly apiKey: string;
  private readonly pollIntervalMs: number;
  private readonly pollTimeoutMs: number;

  constructor(
    private readonly httpService: HttpService,
    private readonly configService: ConfigService,
  ) {
    this.pythonApiUrl = this.configService.get<string>(
      'PYTHON_API_URL',
      'http://localhost:8000',
    );
    this.apiKey = this.configService.get<string>('PYTHON_API_KEY', '');
    this.pollIntervalMs = this.configService.get<number>(
      'PYTHON_JOB_POLL_INTERVAL_MS',
      2_000,
    );
    this.pollTimeoutMs = this.configService.get<number>(
      'PYTHON_JOB_POLL_TIMEOUT_MS',
      600_000, // 10 min ceiling for a retraining run
    );
  }

  private get headers() {
    return this.apiKey ? { 'X-API-Key': this.apiKey } : {};
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  /**
   * Poll the Python service's job-status endpoint until the job reaches a
   * terminal state (`succeeded`/`failed`) or the poll timeout elapses.
   *
   * @param jobId Identifier returned by the submit call.
   * @returns The terminal job-status record.
   * @throws If the job fails, is lost (worker restart), or the timeout elapses.
   */
  private async pollJob<T>(jobId: string): Promise<JobStatusResponse<T>> {
    const deadline = Date.now() + this.pollTimeoutMs;
    for (;;) {
      const response = await firstValueFrom(
        this.httpService.get<JobStatusResponse<T>>(
          `${this.pythonApiUrl}/jobs/${jobId}`,
          { headers: this.headers, timeout: 15_000 },
        ),
      );
      const job = response.data;
      if (TERMINAL_JOB_STATES.includes(job.status)) {
        if (job.status === 'failed') {
          throw new Error(
            `Python job ${jobId} failed: ${job.error ?? 'unknown error'}`,
          );
        }
        return job;
      }
      if (Date.now() >= deadline) {
        throw new Error(
          `Timed out after ${this.pollTimeoutMs}ms waiting for Python job ${jobId} ` +
            `(last status: ${job.status})`,
        );
      }
      await this.sleep(this.pollIntervalMs);
    }
  }

  /**
   * Trigger a retraining run on the Python service.
   * @param force Skip quality gates when true.
   */
  async triggerRetraining(force = false): Promise<RetrainResult> {
    try {
      this.logger.log(`Triggering model retraining (force=${force})`);

      // 1. Submit the job. The Python service enqueues the work and returns
      //    HTTP 202 with a job id immediately instead of blocking.
      const submission = await firstValueFrom(
        this.httpService.post<JobSubmissionResponse>(
          `${this.pythonApiUrl}/retrain`,
          { force },
          { headers: this.headers, timeout: 15_000 },
        ),
      );
      const { job_id: jobId } = submission.data;
      this.logger.log(`Retraining job submitted: job_id=${jobId}`);

      // 2. Poll the job-status endpoint until the run reaches a terminal state.
      const job = await this.pollJob<RetrainResult>(jobId);
      const result = (job.result ?? { status: job.status }) as RetrainResult;
      this.logger.log(
        `Retraining completed: status=${result.status} ` +
          `duration=${result.duration_seconds?.toFixed(1)}s`,
      );
      return result;
    } catch (err) {
      const msg = err instanceof AxiosError ? err.message : String(err);
      this.logger.error(`Retraining request failed: ${msg}`);
      throw err;
    }
  }

  /**
   * Fetch current model registry state and last run metadata.
   */
  async getModelStatus(): Promise<ModelStatusResult> {
    try {
      const response = await firstValueFrom(
        this.httpService.get<ModelStatusResult>(
          `${this.pythonApiUrl}/model/status`,
          { headers: this.headers, timeout: 10_000 },
        ),
      );
      return response.data;
    } catch (err) {
      const msg = err instanceof AxiosError ? err.message : String(err);
      this.logger.error(`Model status request failed: ${msg}`);
      throw err;
    }
  }
}
