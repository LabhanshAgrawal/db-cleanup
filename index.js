// create an express app
const express = require("express")
const app = express()
const { spawn } = require("child_process")

const runCleanup = () => {
  return new Promise((resolve, reject) => {
    let output = '';
    const child = spawn("yarn", ["run", "cleanup"]);
    child.stdout.on("data", (data) => {
      output += data.toString();
      process.stdout.write(data.toString());
    });
    child.on("close", (code) => {
      resolve(output);
    });
  })
}

// define the first route
app.get("/", async function (req, res) {
  const output = await runCleanup();
  res.send("<pre>" + output + "</pre>");
})

// start the server listening for requests
app.listen(process.env.PORT || 3000,
  () => console.log("Server is running..."));