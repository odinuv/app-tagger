'use strict';

import fs from "fs-extra";
import { StorageClient } from "./storage.js";
import { UserException } from "./exceptions.js";
import { Configuration, OpenAIApi } from "openai";
import {COLUMN_METADATA, Metadatum, TABLE_METADATA} from "./metadatum.js";
import {COLUMN_TAG, TABLE_TAG, Tag} from "./tag.js";
import {Table} from "./table.js";

function processConfigurations(componentConfigurations)
{
    const excludedComponents = ['orchestrator', 'keboola.scheduler', 'keboola.sandboxes'];
    const configurations = [];
    componentConfigurations.forEach((componentData) => {
        let configurationsNode = componentData.configurations ?? [];
        configurationsNode.forEach((configurationData) => {
            if (!excludedComponents.some((elm) => elm === componentData.id)) {
                configurations.push({
                    configurationId: configurationData.id,
                    configurationName: configurationData.name,
                    configurationDescription: configurationData.description,
                    componentId: componentData.id,
                    componentName: componentData.name,
                    componentType: componentData.type,
                    componentDescription: componentData.description,
                })
            }
        });
    });
    return configurations;
}

function processTasks(componentConfigurations, configurations)
{
    const orchestratorComponent = componentConfigurations.find(
        (componentData) => componentData.id === 'keboola.orchestrator'
    );
    let configurationsNode = orchestratorComponent.configurations ?? [];
    configurationsNode.forEach((configurationData) => {
        let tasks = configurationData.configuration.tasks ?? [];
        tasks.forEach((task) => {
            let taskConfig = task.task ?? {};
            let index = configurations.findIndex(
                (configuration) =>
                    (configuration.configurationId === taskConfig.configId) &&
                    (configuration.componentId === taskConfig.componentId)
            )
            if (index !== -1) {
                configurations[index].flows = configurations[index].flows ?? new Set();
                configurations[index].flows.add(configurationData.id);
            }
        })
    });
    return configurations;
}

function processTables(tablesData)
{
    let tables = [];
    tablesData.forEach((tableData) => {
        tableData.columns = tableData.columns ?? [];
        tableData.metadata = tableData.metadata ?? [];
        tableData.columnMetadata = tableData.columnMetadata ?? [];

        let columns = [];
        tableData.columns.forEach((columnName) => {
            columns.push({
                name: columnName,
                meta: {}
            })
        });

        let lastUpdatedComponentId = tableData.metadata.find(
            (metadatum) => metadatum.key === 'KBC.lastUpdatedBy.component.id'
        );
        let lastUpdatedConfigurationId = tableData.metadata.find(
            (metadatum) => metadatum.key === 'KBC.lastUpdatedBy.configuration.id'
        );

        tables.push(new Table(
            tableData.id,
            tableData.name,
            lastUpdatedComponentId ? lastUpdatedComponentId.value : null,
            lastUpdatedConfigurationId ? lastUpdatedConfigurationId.value : null,
            columns
        ));
    });
    return tables;
}

async function processSampleData(tables, storage)
{
    return await Promise.all(tables.map(async (table) => {
        let sampleData = await storage.dataPreview(table.id);
        let rows = sampleData.rows ?? [];
        rows.forEach((sampleRow) => {
            sampleRow.forEach((sampleColumn) => {
                let columnIndex = table.columns.findIndex((column) => column.name === sampleColumn.columnName);
                table.columns[columnIndex].sampleValues = table.columns[columnIndex].sampleValues ?? new Set();
                if (table.columns[columnIndex].sampleValues.size < 5) {
                    table.columns[columnIndex].sampleValues.add(sampleColumn.value.substring(0, 100));
                }
            });
        });
        return table;
    }));
}

async function prepareTablePrompt(explanations, configurations, table)
{
    let explanationsPrompt = '';
    explanations.forEach((explanation) => {
        explanationsPrompt += `"${explanation.source}" means ${explanation.explanation}.\n`
    });

    let configuration;
    if (table.lastUpdatedComponentId) {
        configuration = configurations.find((configuration) =>
            configuration.componentId === table.lastUpdatedComponentId &&
            configuration.configurationId === table.lastUpdatedConfigurationId
        );
    }

    let flowPrompt = '';
    if (configuration) {
        const flowsIt = configuration.flows.values();
        if (flowsIt) {
            let val = flowsIt.next().value;
            let flow = configurations.find((configuration) => {
                return configuration.componentId === 'keboola.orchestrator' &&
                    configuration.configurationId === val;
            });
            let description = flow.configurationDescription.replace(/\r?\n|\r/g, ' ');
            flowPrompt = `There is a flow named "${flow.configurationName}" with description "${description}".`
        }
    }

    let columnsPrompt = '';
    table.columns.forEach((column) => {
        if (column.sampleValues) {
            let values = [...column.sampleValues].join(', ');
            columnsPrompt += `The column "${column.name}" with sample values: ${values}\n`;
        } else {
            columnsPrompt += `The column "${column.name}".\n`;
        }
    });

    let tablePrompt = `The table "${table.id}" contains the above columns.`;
    if (configuration) {
        tablePrompt += `\nThe table "${table.id}" is produced by "${configuration.configurationName}" ${configuration.componentName} ${configuration.componentType}.`;
    }

    return `
"""
${explanationsPrompt}
${flowPrompt}

${columnsPrompt}

${tablePrompt}
"""`
}

async function createCompletion(prompt, size, maxTokens = 50)
{
    //console.log(prompt);
    const openApiConfiguration = new Configuration({
        apiKey: process.env.OPENAI_API_KEY,
    });
    const openai = new OpenAIApi(openApiConfiguration);
    const response = await openai.createCompletion({
        model: "text-davinci-003",
        prompt: prompt,
        temperature: 0,
        max_tokens: maxTokens,
        frequency_penalty: 2,
        presence_penalty: 1.5,
    });

    //console.log(response.data.choices);
    let text = response.data.choices[0].text ?? '';
    let matches = text.matchAll(/\{(.*?)}/g);
    let words = [];
    for (const match of matches) {
        words.push(match[1].trim());
    }

    if (words && (words.length === size || size === 0)) {
        return words;
    } else {
        console.log(`Size mismatch, expected ${size}, got ${words.join()}.`);
        return Array(size).fill('N/A');
    }
}

function initialize()
{
    const dataDir = process.env.KBC_DATADIR + '/' ?? '/data/';
    const storageApiUrl = process.env.KBC_URL ?? null;
    const storageApiToken = process.env.KBC_TOKEN ?? null;
    const branchId = process.env.KBC_BRANCHID ?? null;

    if (storageApiToken === null) {
        throw new Error('Storage API token is missing from environment variable KBC_TOKEN.');
    }
    if (storageApiUrl === null) {
        throw new Error('Storage API URL is missing from environment variable KBC_URL.');
    }
    if (branchId !== null) {
        throw new UserException('Component cannot run in branch.');
    }

    console.log('Data directory: ', dataDir);
    return {
        dataDir,
        storageApiUrl,
        storageApiToken,
        branchId,
    };
}

async function getConfigData(dataDir)
{
    return await fs.readJson(dataDir + 'config.json', 'utf8');
}

async function getConfigurations(storage)
{
    const componentConfigurations = await storage.listConfigurations();
    let configurations = processConfigurations(componentConfigurations);
    return processTasks(componentConfigurations, configurations);
}

async function getTables(storage, useDataPreviews)
{
    const tablesData = await storage.listTables();
    let tables = processTables(tablesData);
    if (useDataPreviews) {
        return await processSampleData(tables, storage);
    } else {
        return tables;
    }
}

async function getExplanations(configData)
{
    return configData.parameters.explanations ?? [];
}

async function filterTables(tables, configurations, includeFlows, excludeTables)
{
    if (includeFlows) {
        tables = tables.filter((table) => {
            if (!table.lastUpdatedConfigurationId || !table.lastUpdatedComponentId) {
                console.log(`Table "${table.id}" excluded because no configuration is associated to it.`);
                return false;
            }
            let configuration = configurations.find((configuration) =>
                configuration.configurationId === table.lastUpdatedConfigurationId &&
                configuration.componentId === table.lastUpdatedComponentId
            );
            if (!configuration) {
                console.log(
                    `Table "${table.id}" excluded because configuration ${table.lastUpdatedConfigurationId} of component ${table.lastUpdatedComponentId} does not exist.`
                );
                return false;
            }
            if (configuration.flows) {
                for (const flowId of configuration.flows.values()) {
                    if (includeFlows.indexOf(flowId) > -1) {
                        console.log(`Table "${table.id}" belongs to flow "${flowId}".`);
                        return true;
                    }
                }
            }
            console.log(`Table "${table.id}" excluded because none of the flows is matched.`);
            return false;
        });
    }

    if (excludeTables) {
        excludeTables.forEach((exclusion) => {
            let regexp = new RegExp(exclusion, 'g');
            tables = tables.filter((table) => {
                if (regexp.exec(table.id)) {
                    console.log(`Table "${table.id}" excluded by pattern "${exclusion}"`);
                    return false;
                }
                return true;
            });
        });
    }
    return tables;
}

async function generateTablesMetadata(tables, configurations, explanations)
{
    let tableMetadata = {};
    let columnsMetadata = {};
    let tags = [];
    await Promise.all(tables.map(async (table) => {
        let promptBase = await prepareTablePrompt(explanations, configurations, table);
        let tablePrompt = promptBase +
            `
Generate two labels for the table representing the content and role.
Insert each label in curly braces.

content: {...}
role: {...}
`
        let [content, role] = await createCompletion(tablePrompt, 2);
        tableMetadata[table.id] = tableMetadata[table.id] ?? [];
        tableMetadata[table.id].push(new Metadatum(table.id, TABLE_METADATA, 'KBC.guessed.role', role));
        tableMetadata[table.id].push(new Metadatum(table.id, TABLE_METADATA, 'KBC.guessed.content', content));
        tags.push(new Tag(table.id, TABLE_TAG, `${role} ${content}`));
        await Promise.all(table.columns.map(async (column) => {
            let columnPrompt = promptBase +
                `
Generate three labels for the column "${column.name}" representing the content, category, data type.
Insert each label in curly braces.

content: {...}
category: {...}
data type: {...}
`
            let [content, category, dataType] = await createCompletion(columnPrompt, 3);
            let columnId = `${table.id}.${column.name}`;
            columnsMetadata[table.id] = columnsMetadata[table.id] ?? [];
            columnsMetadata[table.id].push(new Metadatum(columnId, COLUMN_METADATA, 'KBC.guessed.role', content, column.name));
            columnsMetadata[table.id].push(new Metadatum(columnId, COLUMN_METADATA, 'KBC.guessed.content', category, column.name));
            columnsMetadata[table.id].push(new Metadatum(columnId, COLUMN_METADATA, 'KBC.guessed.dataType', dataType, column.name));
            tags.push(new Tag(columnId, COLUMN_TAG, `${category} ${content}`));
        }));
    }));
    return {tableMetadata, columnsMetadata, tags};
}

async function generateCategories(tags)
{
    let newTags = [];
    let tagsSlice = [];
    let i = 0;
    do {
        tagsSlice = tags.slice(200 * i, 200 * (i + 1));
        let tagPrompt = '';
        if (tagsSlice.length) {
            tagsSlice.forEach((tag) => {
                tagPrompt += `${tag.source}\n`;
            });
            tagPrompt += `Assign the above items into 10 categories. List only the categories. Enclose each category in curly braces.`;
            let newTagsSlice = await createCompletion(tagPrompt, 0, 200);
            newTags.push(...newTagsSlice);
        }

        i++;
    } while (tagsSlice.length);
    return newTags;
}

async function assignCategories(newTags, tableMetadata)
{
    let categoryPromptBase = `${newTags.join('\n')}\nAssign two of the above categories to the following object. Enclose each category in curly braces.\n 1. category:\n 2. category:`;

    await Promise.all(Object.entries(tableMetadata).map(async (item) => {
        let tableId = item[0];
        let metadata = item[1];
        let categoryPrompt = categoryPromptBase;
        metadata.forEach((value) => {
            categoryPrompt += `${value.key}: ${value.value}\n`;
        });
        let categories = await createCompletion(categoryPrompt, 2);

        tableMetadata[tableId].push(new Metadatum(tableId, TABLE_METADATA, 'KBC.guessed.category1', categories[0]));
        tableMetadata[tableId].push(new Metadatum(tableId, TABLE_METADATA, 'KBC.guessed.category2', categories[1]));
    }));
    return tableMetadata;
}

async function writeMetadata(tableMetadata, columnsMetadata, writeData, storage)
{
    await Promise.all(Object.entries(tableMetadata).map(async (item) => {
        let tableId = item[0];
        let metadata = item[1];
        let tableMetadataProcessed = [];
        metadata.forEach((metadatum) => {
            console.log(`Setting table "${tableId}' metadata "${metadatum.key}=${metadatum.value}"`);
            tableMetadataProcessed.push({ key: metadatum.key, value: metadatum.value});
        });
        let columnsMetadataProcessed = {};
        columnsMetadata[tableId].forEach((metadatum) => {
            console.log(`Setting column "${metadatum.id}' metadata "${metadatum.key}=${metadatum.value}"`);
            columnsMetadataProcessed[metadatum.name] = columnsMetadataProcessed[metadatum.name] ?? [];
            columnsMetadataProcessed[metadatum.name].push({ key: metadatum.key, value: metadatum.value});
        });

        if (writeData) {
            console.log(`Writing table ${tableId} metadata.`);
            let ret = await storage.setTableMetadata(tableId, tableMetadataProcessed, columnsMetadataProcessed);
            //freeze(1000);
        }
    }));
}

function freeze(time) {
    const stop = new Date().getTime() + time;
    while(new Date().getTime() < stop);
}

export async function run () {
    const {dataDir, storageApiUrl, storageApiToken, branchId} = initialize();
    const configData = await getConfigData(dataDir);
    const storage = new StorageClient(storageApiUrl, storageApiToken);
    const explanations = await getExplanations(configData);
    const includeFlows = configData.parameters.includeFlows || [];
    const excludeTables = configData.parameters.excludeTables || [];
    const useDataPreviews = configData.parameters.useDataPreviews || false;
    const writeData = configData.parameters.writeData || false;
    const openApiToken = configData.parameters['#openApiKey'] || null;
    // todo
    process.env.OPENAI_API_KEY = openApiToken;
    if (!openApiToken) {
        throw new UserException('#openApiToken must be specified in parameters.');
    }

    const configurations = await getConfigurations(storage);
    let tables = await getTables(storage, useDataPreviews);
    tables = await filterTables(tables, configurations, includeFlows, excludeTables);

    // generate completion for all tables
    let {tableMetadata, columnsMetadata, tags} = await generateTablesMetadata(tables, configurations, explanations);
    let newTags = await generateCategories(tags);

    tableMetadata = await assignCategories(newTags, tableMetadata);
    await writeMetadata(tableMetadata, columnsMetadata, writeData, storage);
    process.exit(0);
}
