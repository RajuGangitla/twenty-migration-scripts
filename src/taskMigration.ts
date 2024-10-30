import axios, { AxiosError, AxiosRequestConfig } from 'axios';
import dotenv from 'dotenv';
import rateLimit from 'axios-rate-limit';
import { createLogger, format, transports } from 'winston';
import { z } from 'zod';

// Load environment variables
dotenv.config();

// Configure logging
const logger = createLogger({
    level: 'info',
    format: format.combine(
        format.timestamp(),
        format.json()
    ),
    transports: [
        new transports.File({ filename: 'error.log', level: 'error' }),
        new transports.File({ filename: 'migration.log' }),
        new transports.Console()
    ]
});

// Validation schemas
const EnvSchema = z.object({
    ZOHO_BASE_URL: z.string().url(),
    ZOHO_API_KEY: z.string().min(1),
    TWENTY_BASE_URL: z.string().url(),
    TWENTY_API_KEY: z.string().min(1),
    BATCH_SIZE: z.string().transform(Number).default('50'),
    RATE_LIMIT_PER_SECOND: z.string().transform(Number).default('5')
});

// Types
type ZohoTask = {
    id: string;
    Subject: string;
    Status: string;
    Due_Date: string;
    Owner?: {
        id: string;
        name: string;
    };
    Modified_Time?: string;
    Created_Time?: string;
};


type TwentyTask = {
    title: string;
    status: string;
    dueAt: string;
    position: number;
};

// Create rate-limited axios instances
const createAxiosInstance = (baseURL: string, apiKey: string, isZoho: boolean) => {
    const config: AxiosRequestConfig = {
        baseURL,
        timeout: 10000,
        headers: {
            'Authorization': isZoho
                ? `Zoho-oauthtoken ${apiKey}`
                : `Bearer ${apiKey}`
        }
    };

    return rateLimit(
        axios.create(config),
        { maxRPS: Number(process.env.RATE_LIMIT_PER_SECOND) || 5 }
    );
};

// Error handling
const handleApiError = (error: any) => {
    if (error) {
        const statusCode = error.response?.status || 500;
        const message = error.response?.data?.message || error.message;
        logger.error('API Error', { statusCode, message });
        throw new Error(`API Error: ${message}`);
    }
    throw error;
};

// Zoho API functions
const fetchZohoTasks = async (axiosInstance: any): Promise<ZohoTask[]> => {
    try {
        const response = await axiosInstance.get('/crm/v2/Tasks');
        return response.data.data;
    } catch (error) {
        return handleApiError(error);
    }
};

// Twenty API functions
const createTwentyTasks = async (axiosInstance: any, tasks: TwentyTask[]): Promise<void> => {
    try {
        console.log(tasks, "tasks")
        await axiosInstance.post('/batch/tasks', tasks);
    } catch (error) {
        handleApiError(error);
    }
};

// Mapping functions
const mapStatus = (zohoStatus: string): string => {
    const statusMap: Record<string, string> = {
        'Open': 'TODO',
        'In Progress': 'IN_PROGESS',
        'Completed': 'DONE',
    };
    return statusMap[zohoStatus] || 'TODO';
};

const validateDate = (date: string): string => {
    const parsed = new Date(date);
    return isNaN(parsed.getTime()) ? new Date().toISOString() : parsed.toISOString();
};

const mapToTwentyTask = (zohoTask: ZohoTask, position: number): TwentyTask => ({
    title: zohoTask.Subject,
    status: mapStatus(zohoTask.Status),
    dueAt: validateDate(zohoTask.Due_Date),
    position,
});

// Batch processing function
const processBatch = async (
    tasks: ZohoTask[],
    startIndex: number,
    batchSize: number,
    twentyAxios: any
): Promise<void> => {
    const batch = tasks.slice(startIndex, startIndex + batchSize);
    const twentyTasks = batch.map((task, index) =>
        mapToTwentyTask(task, startIndex + index + 1)
    );

    await createTwentyTasks(twentyAxios, twentyTasks);
    logger.info(`Processed batch ${Math.floor(startIndex / batchSize) + 1}`);
    await new Promise(resolve => setTimeout(resolve, 1000)); // Delay between batches
};

// Main migration function
const migrateTasks = async (config: z.infer<typeof EnvSchema>): Promise<void> => {
    const zohoAxios = createAxiosInstance(config.ZOHO_BASE_URL, config.ZOHO_API_KEY, true);
    const twentyAxios = createAxiosInstance(config.TWENTY_BASE_URL, config.TWENTY_API_KEY, false);

    try {
        logger.info('Starting task migration...');

        // Fetch all tasks from Zoho
        const zohoTasks = await fetchZohoTasks(zohoAxios);
        logger.info(`Fetched ${zohoTasks.length} tasks from Zoho CRM`);

        // Process tasks in batches
        for (let i = 0; i < zohoTasks.length; i += config.BATCH_SIZE) {
            await processBatch(zohoTasks, i, config.BATCH_SIZE, twentyAxios);
        }

        logger.info('Migration completed successfully');
    } catch (error) {
        logger.error('Migration failed', { error });
        throw error;
    }
};

// Main execution function
const main = async () => {
    try {
        // Validate environment variables
        const config = EnvSchema.parse(process.env);
        await migrateTasks(config);
    } catch (error) {
        if (error instanceof z.ZodError) {
            logger.error('Environment validation failed', { error: error.errors });
        } else {
            logger.error('Unexpected error', { error });
        }
        process.exit(1);
    }
};

// Execute if run directly
main();
