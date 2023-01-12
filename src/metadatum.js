'use strict';

export const TABLE_METADATA = 'table';
export const COLUMN_METADATA = 'column';

export class Metadatum {
    constructor(id, kind, key, value, name) {
        this.id = id;
        this.kind = kind;
        this.key = key;
        this.value = value;
        this.name = name;
    }
}
