// create an express app
const express = require("express")
const app = express()
const { execSync } = require("child_process")

// define the first route
app.get("/", function (req, res) {
  res.send("<pre>" + execSync('yarn run cleanup').toString() + "</pre>");
})

// start the server listening for requests
app.listen(process.env.PORT || 3000,
  () => console.log("Server is running..."));