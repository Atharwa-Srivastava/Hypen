require("dotenv").config();
const { runCluster } = require("./lib/hardening");

runCluster(() => {
  require("./server");
});
