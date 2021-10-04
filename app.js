const express = require('express')
const createError = require("http-errors");
const path = require("path");
const route = require('./route.js');

const app = express();
const port = 8888;

app.set("views", path.join(__dirname, "views"));
app.set("view engine", "pug");

app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(express.static(path.join(__dirname, "public")));

app.use('/',route);

app.listen(port, () => {
  console.log(`Example app listening at http://localhost:${port}`);
});