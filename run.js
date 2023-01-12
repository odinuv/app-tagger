'use strict';

import * as Component from "./src/component.js";
import {UserException} from "./src/exceptions.js";

try {
    await Component.run();
} catch (e) {
    console.log(e);
    if (e instanceof UserException) {
        process.exit(1);
    }
    process.exit(2);
}
