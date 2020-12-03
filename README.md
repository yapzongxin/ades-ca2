# Backend

![](https://img.shields.io/badge/Graded-CA1-critical)

Build the backend following the API specification listed in the [module website](http://ades-fsp.github.io)

## Github Classroom Link

1. [ADES-01](https://classroom.github.com/g/GmkHwJCB)
2. [ADES-02](https://classroom.github.com/g/T1omMSNJ)
3. [ADES-03](https://classroom.github.com/g/U0soTvDO)
4. [ADES-04](https://classroom.github.com/g/3vbhTdcw)

## Instructions 

Refer back to the [Project Introduction Document](https://ades-fsp.github.io/#project-introduction) to better understand how this application will be used by the different users. 

Some files have already been created for you: 

1. `app.js`

    You should implement the routes in this file, you should **NOT** run `app.listen(...)` in this file, you are encouraged to create another file, import app and start the server there. Example: 

    ```js
    const { app } = require('./app');
    app.listen(3000, () => console.log("App listening on port 3000"));
    ``` 

2. `database.js`

    Any database related code should go here. There are 2 function defined `resetTables` and `closeDatabaseConnections`. 
    
    The `resetTables` when called should return a promise that resolves when the database is successfully reset, and rejects if there was any error. 

    The `closeDatabaseConnections` when called should return a promise that resolves when all connection to the database is successfully closed, and rejects if there was any error. 


## Running Tests

There are several tests in this repository (`./test/*.test.js`). And each of this tests the correctness of your API schema and **NOT THE BEHAVIOR OF YOUR API**, you should design your own test cases to test the behavior of your API.

You are not to modify the test files, also prevent naming files similar to the test files.

-   Check Queue API

    ```
    npm test -- checkQueue
    ```

-   Update Queue API

    ```
    npm test -- updateQueue
    ```

-   Join Queue API

    ```
    npm test -- joinQueue
    ```

-   Server Available API

    ```
    npm test -- serverAvailable
    ```

-   Check Queue API

    ```
    npm test -- checkQueue
    ```
