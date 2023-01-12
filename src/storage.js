'use strict';

import https from 'https';
import {UserException} from "./exceptions.js";

export class StorageClient {
    constructor(url, token) {
        this.url = url;
        this.token = token
    }

    async sendRequest(options, body) {
        options.headers = options.headers ?? {};
        options.headers['x-storageapi-token'] = this.token;
        return new Promise((resolve, reject) => {
            const req = https.request(this.url, options, res => {
                let rawData = '';

                res.on('data', chunk => {
                    rawData += chunk;
                });

                res.on('end', () => {
                    try {
                        resolve(JSON.parse(rawData));
                    } catch (err) {
                        reject(new UserException(err.message));
                    }
                });
            });

            req.on('error', err => {
                reject(new UserException(err.message));
            });
            if (body) {
                req.write(JSON.stringify(body));
            }
            req.end();
        });
    }
    async listConfigurations() {
        return this.sendRequest(
            {
                path: '/v2/storage/branch/default/components?include=configuration',
                method: 'GET',
            }
        );
    }

    async listTables() {
        return this.sendRequest(
            {
                path: '/v2/storage/tables?include=metadata,columns,columnMetadata',
                method: 'GET',
            }
        );
    }

    async dataPreview(id) {
        return this.sendRequest(
            {
                path: `/v2/storage/tables/${id}/data-preview/?limit=100&format=json`,
                method: 'GET'
            }
        )
    }

    async setTableMetadata(tableId, tableMetadata, columnsMetadata) {
        return this.sendRequest(
            {
                path: `/v2/storage/tables/${tableId}/metadata`,
                method: 'POST',
                headers: {'Content-Type': 'application/json'}
            },
            {
                provider: 'app-tagger',
                metadata: tableMetadata,
                columnsMetadata: columnsMetadata,
            }
        );
    }
}
