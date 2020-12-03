const { resetTables, closeDatabaseConnections } = require('../database');
const chalk = require('chalk');

resetTables()
    .then(function (res) {
        console.log(chalk.green('Success!'));
    })
    .catch(console.error)
    .finally(closeDatabaseConnections);
