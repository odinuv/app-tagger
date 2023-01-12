'use strict';
export class UserException extends Error {
    constructor(message) {
        super(message);
        this.name = 'UserException';
    }
}
