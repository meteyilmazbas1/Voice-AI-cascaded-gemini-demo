const { spawn } = require("node:child_process");
const http = require("node:http");

const PORT = process.env.PORT || "3010";
const child = spawn(process.execPath, ["server.js"], {
    env: {
        ...process.env,
        PORT,
        PUBLIC_URL: `http://127.0.0.1:${PORT}`,
        DEEPGRAM_API_KEY: "dummy",
        ANTHROPIC_API_KEY: "dummy",
        ELEVENLABS_API_KEY: "dummy",
        PUBLIC_URL: "http://localhost:" + PORT,
        SUPABASE_URL: "https://dummy.supabase.co",
        SUPABASE_SERVICE_KEY: "dummy-key",
        NODE_ENV: "test",
    },
    stdio: "inherit",
});

const deadline = Date.now() + 15000;
(function poll() {
    http.get(`http://127.0.0.1:${PORT}/`, (res) => {
        console.log(`✓ Server responded: ${res.statusCode}`);
        child.kill("SIGTERM");
        process.exit(0);
    }).on("error", () => {
        if (Date.now() > deadline) {
            console.error("✗ Server did not respond within 15s");
            child.kill("SIGTERM");
            process.exit(1);
        }
        setTimeout(poll, 500);
    });
})();