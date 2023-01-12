'use strict';

export class Table {
    constructor(id, name, lastUpdatedComponentId, lastUpdatedConfigurationId, columns) {
        this.id = id;
        this.name = name;
        this.lastUpdatedComponentId = lastUpdatedComponentId;
        this.lastUpdatedConfigurationId = lastUpdatedConfigurationId;
        this.columns = columns;
    }
}
