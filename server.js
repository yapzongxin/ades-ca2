const { app } = require("./app");
// const port = 3000; // Fixed at port 3000
const port = process.env.PORT || 3000; // Look for port number in environment, otherwise, default at 3000
app.listen(port, () => {});
