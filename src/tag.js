'use strict';

export const TABLE_TAG = 'table';
export const COLUMN_TAG = 'column';

export class Tag {
    constructor(id, kind, source) {
        this.id = id;
        this.kind = kind;
        this.source = source;
    }
}
