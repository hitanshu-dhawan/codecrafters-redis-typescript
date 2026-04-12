import * as net from "net";

/** In-memory key-value store. Each entry holds the value and an optional expiry timestamp (epoch ms). */
const store = new Map<string, { value: string; expiresAt: number | null }>();

/** In-memory list store. Each entry holds an ordered array of strings. */
const lists = new Map<string, string[]>();

/**
 * Parses a RESP (Redis Serialization Protocol) message into an array of string arguments.
 *
 * RESP arrays follow the format: `*<count>\r\n` followed by `<count>` bulk strings.
 * Each bulk string follows the format: `$<length>\r\n<data>\r\n`.
 *
 * @param data - The raw RESP-encoded string received from the client.
 * @returns An array of parsed string arguments (e.g., `["ECHO", "hey"]`).
 *
 * @example
 * parseRESP("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n");
 * // Returns: ["ECHO", "hey"]
 */
function parseRESP(data: string): string[] {
    // Split the raw data by CRLF to get individual RESP tokens
    const lines = data.split("\r\n");
    const args: string[] = [];
    let i = 0;

    // Check if the message is a RESP array (indicated by the '*' prefix)
    if (lines[i].startsWith("*")) {
        // Extract the number of elements in the array
        const count = parseInt(lines[i].substring(1));
        i++;

        // Iterate through each expected element
        for (let j = 0; j < count; j++) {
            // Each element should be a bulk string (indicated by the '$' prefix)
            if (lines[i].startsWith("$")) {
                i++; // Skip the byte-length line (e.g., "$4")
                args.push(lines[i]); // Capture the actual string value (e.g., "ECHO")
                i++;
            }
        }
    }
    return args;
}

/**
 * Handles an incoming client connection by parsing RESP commands and
 * dispatching the appropriate response.
 *
 * Supported commands:
 * - `PING` → Responds with `+PONG\r\n` (RESP simple string).
 * - `ECHO <arg>` → Responds with the argument as a RESP bulk string.
 * - `SET <key> <value> [PX ms | EX s]` → Stores the key-value pair (with optional expiry) and responds with `+OK\r\n`.
 * - `GET <key>` → Responds with the value as a RESP bulk string, or `$-1\r\n` if not found or expired.
 * - `RPUSH <key> <element> [element ...]` → Appends one or more elements to the end of a list (creates if missing). Returns list length as a RESP integer.
 * - `LPUSH <key> <element> [element ...]` → Prepends one or more elements to the start of a list (creates if missing). Returns list length as a RESP integer.
 * - `LRANGE <key> <start> <stop>` → Returns a range of elements from a list as a RESP array. Supports negative indexes.
 * - `LLEN <key>` → Returns the length of a list as a RESP integer (0 if the list doesn't exist).
 * - `LPOP <key> [count]` → Removes and returns the first element(s) from a list. Without count: returns a bulk string. With count: returns a RESP array.
 *
 * @param connection - The TCP socket for the connected client.
 */
function handleConnection(connection: net.Socket): void {
    connection.on("data", (data: Buffer) => {
        // Decode the buffer and parse the RESP message into command arguments
        const args = parseRESP(data.toString());

        // Normalize the command name to uppercase (Redis commands are case-insensitive)
        const command = args[0]?.toUpperCase();

        if (command === "PING") {
            // PING: respond with a simple string "+PONG"
            connection.write("+PONG\r\n");
        } else if (command === "ECHO") {
            // ECHO: echo back the first argument as a bulk string "$<len>\r\n<arg>\r\n"
            const arg = args[1];
            connection.write(`$${arg.length}\r\n${arg}\r\n`);
        } else if (command === "SET") {
            // SET: store the key-value pair, with optional PX (ms) or EX (s) expiry
            const key = args[1];
            const value = args[2];
            let expiresAt: number | null = null;

            // Check for optional expiry arguments (case-insensitive)
            for (let i = 3; i < args.length; i++) {
                const option = args[i].toUpperCase();
                if (option === "PX" && i + 1 < args.length) {
                    // PX: expiry in milliseconds
                    expiresAt = Date.now() + parseInt(args[i + 1]);
                    break;
                } else if (option === "EX" && i + 1 < args.length) {
                    // EX: expiry in seconds
                    expiresAt = Date.now() + parseInt(args[i + 1]) * 1000;
                    break;
                }
            }

            store.set(key, { value, expiresAt });
            connection.write("+OK\r\n");
        } else if (command === "GET") {
            // GET: retrieve the value for the given key, checking for expiry
            const key = args[1];
            const entry = store.get(key);

            if (entry === undefined) {
                // Key does not exist
                connection.write("$-1\r\n");
            } else if (entry.expiresAt !== null && Date.now() > entry.expiresAt) {
                // Key has expired — remove it and return null
                store.delete(key);
                connection.write("$-1\r\n");
            } else {
                // Key exists and is still valid
                connection.write(`$${entry.value.length}\r\n${entry.value}\r\n`);
            }
        } else if (command === "RPUSH") {
            // RPUSH: append one or more elements to the end of a list, creating it if it doesn't exist
            const key = args[1];
            const elements = args.slice(2);
            let list = lists.get(key);
            if (!list) {
                // List doesn't exist yet — create an empty one
                list = [];
                lists.set(key, list);
            }
            // Append all elements to the tail of the list
            list.push(...elements);
            // Respond with the new list length as a RESP integer ":N\r\n"
            connection.write(`:${list.length}\r\n`);
        } else if (command === "LPUSH") {
            // LPUSH: prepend one or more elements to the start of a list, creating it if it doesn't exist
            const key = args[1];
            const elements = args.slice(2);
            let list = lists.get(key);
            if (!list) {
                // List doesn't exist yet — create an empty one
                list = [];
                lists.set(key, list);
            }
            // Insert each element at the head one by one, so the last argument ends up first
            // e.g. LPUSH key "a" "b" "c" → list becomes ["c", "b", "a"]
            for (const el of elements) {
                list.unshift(el);
            }
            // Respond with the new list length as a RESP integer ":N\r\n"
            connection.write(`:${list.length}\r\n`);
        } else if (command === "LRANGE") {
            // LRANGE: return a range of elements from a list (inclusive start and stop)
            const key = args[1];
            let start = parseInt(args[2]);
            let stop = parseInt(args[3]);
            const list = lists.get(key);

            if (!list || list.length === 0) {
                // List doesn't exist or is empty — respond with an empty RESP array
                connection.write("*0\r\n");
            } else {
                const len = list.length;

                // Resolve negative indexes: -1 means last element, -2 second-to-last, etc.
                if (start < 0) start = Math.max(start + len, 0);
                if (stop < 0) stop = stop + len;

                // Clamp stop to the last valid index
                if (stop >= len) stop = len - 1;

                if (start > stop || start >= len) {
                    // Range is empty — respond with an empty RESP array
                    connection.write("*0\r\n");
                } else {
                    // Extract the requested slice (stop is inclusive, so +1 for Array.slice)
                    const slice = list.slice(start, stop + 1);
                    // Build a RESP array: "*<count>\r\n" followed by bulk strings
                    let resp = `*${slice.length}\r\n`;
                    for (const el of slice) {
                        resp += `$${el.length}\r\n${el}\r\n`;
                    }
                    connection.write(resp);
                }
            }
        } else if (command === "LLEN") {
            // LLEN: return the length of a list (0 if the list doesn't exist)
            const key = args[1];
            const list = lists.get(key);
            connection.write(`:${list ? list.length : 0}\r\n`);
        } else if (command === "LPOP") {
            // LPOP: remove and return element(s) from the head of a list
            const key = args[1];
            const list = lists.get(key);

            if (!list || list.length === 0) {
                // List doesn't exist or is empty — respond with a null bulk string
                connection.write("$-1\r\n");
            } else if (args.length > 2) {
                // Count argument provided — remove up to `count` elements and return as a RESP array
                const count = Math.min(parseInt(args[2]), list.length);
                const removed = list.splice(0, count);
                let resp = `*${removed.length}\r\n`;
                for (const el of removed) {
                    resp += `$${el.length}\r\n${el}\r\n`;
                }
                connection.write(resp);
            } else {
                // No count argument — remove and return the first element as a bulk string
                const el = list.shift()!;
                connection.write(`$${el.length}\r\n${el}\r\n`);
            }
        }
    });
}

// Initialize the TCP server and register the connection handler
const server: net.Server = net.createServer(handleConnection);

// Bind to localhost on the default Redis port (6379)
server.listen(6379, "127.0.0.1");
