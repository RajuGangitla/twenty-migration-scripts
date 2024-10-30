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

type ZohoContact = {
    id: string;
    First_Name: string;
    Last_Name: string;
    Email: string;
    Phone: string;
    Mobile: string;
    Owner?: {
        name: string;
        id: string;
        email: string;
    };
    Created_By: {
        name: string;
        id: string;
        email: string;
    };
    Mailing_City: string;
    Title: string;
    Record_Image: string; // Avatar URL
    Mailing_Country: string; // Assuming this can be used for country code or validation
    // Additional fields...
};

type TwentyContact = {
    name: {
        firstName: string;
        lastName: string;
    };
    emails: {
        primaryEmail: string;
        additionalEmails?: string[];
    };
    linkedinLink?: {
        primaryLinkLabel?: string;
        primaryLinkUrl?: string;
        secondaryLinks?: { label: string; url: string }[];
    };
    xLink?: {
        primaryLinkLabel?: string;
        primaryLinkUrl?: string;
        secondaryLinks?: { label: string; url: string }[];
    };
    jobTitle?: string;
    phones: {
        primaryPhoneNumber: string;
        primaryPhoneCountryCode?: string;
        additionalPhones?: string[];
    };
    city?: string;
    avatarUrl?: string; // Assuming you have a way to get this
    position: number; // Position to be set according to your logic
    createdBy: {
        source: string
    };
    companyId?: string; // Assuming you might have this information
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
const fetchZohoContacts = async (axiosInstance: any): Promise<ZohoContact[]> => {
    try {
        const response = await axiosInstance.get('/crm/v2/Contacts');
        console.log(response.data.data, "response.data.data")
        return response.data.data;
    } catch (error) {
        return handleApiError(error);
    }
};

// Twenty API functions
const createTwentyContacts = async (axiosInstance: any, contacts: TwentyContact[]): Promise<void> => {
    try {
        await axiosInstance.post('/batch/people', contacts);
    } catch (error) {
        handleApiError(error);
    }
};

// Mapping functions
const mapToTwentyContact = (zohoContact: ZohoContact, index: number): TwentyContact => ({
    name: {
        firstName: zohoContact.First_Name,
        lastName: zohoContact.Last_Name,
    },
    emails: {
        primaryEmail: zohoContact.Email,
        additionalEmails: [] // You can add logic if there are additional emails
    },
    linkedinLink: {
        // Placeholder for LinkedIn mapping logic
    },
    jobTitle: zohoContact.Title,
    phones: {
        primaryPhoneNumber: zohoContact.Phone,
        primaryPhoneCountryCode: zohoContact.Mailing_Country, // Assuming this is valid for country code
        additionalPhones: [zohoContact.Mobile] // Use Mobile as an additional phone number
    },
    city: zohoContact.Mailing_City,
    position: index + 1, // Set the position dynamically based on index
    createdBy: {
        source: 'EMAIL', // You can change this based on your source logic
    },
});

// Batch processing function
const processBatch = async (
    contacts: ZohoContact[],
    startIndex: number,
    batchSize: number,
    twentyAxios: any
): Promise<void> => {
    const batch = contacts.slice(startIndex, startIndex + batchSize);
    const twentyContacts = batch.map(mapToTwentyContact);
    await createTwentyContacts(twentyAxios, twentyContacts);
    logger.info(`Processed batch ${Math.floor(startIndex / batchSize) + 1}`);
    await new Promise(resolve => setTimeout(resolve, 1000)); // Delay between batches
};

// Main migration function
const migrateContacts = async (config: z.infer<typeof EnvSchema>): Promise<void> => {
    const zohoAxios = createAxiosInstance(config.ZOHO_BASE_URL, config.ZOHO_API_KEY, true);
    const twentyAxios = createAxiosInstance(config.TWENTY_BASE_URL, config.TWENTY_API_KEY, false);

    try {
        logger.info('Starting contact migration...');

        // Fetch all contacts from Zoho
        const zohoContacts = await fetchZohoContacts(zohoAxios);
        logger.info(`Fetched ${zohoContacts.length} contacts from Zoho CRM`);

        // Process contacts in batches
        for (let i = 0; i < zohoContacts.length; i += config.BATCH_SIZE) {
            await processBatch(zohoContacts, i, config.BATCH_SIZE, twentyAxios);
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
        await migrateContacts(config);
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
