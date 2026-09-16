/**
 * VIGS — Visual Impaired Guide System
 * Serial-to-WebSocket Bridge Server
 * 
 * This Node.js server reads data from the Arduino over USB serial port
 * and broadcasts it in real-time to the webpage via WebSocket.
 * 
 * SETUP:
 *   1. npm install
 *   2. node server.js
 *   3. Open vigs.html in your browser
 */

const { SerialPort } = require('serialport');
const { ReadlineParser } = require('@serialport/parser-readline');
const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

// ─── CONFIG ─────────────────────────────────────────────────────────────────
const WS_PORT   = 8080;
const HTTP_PORT = 3000;
const BAUD_RATE = 9600;

// Auto-detect Arduino port or set manually here:
// e.g. 'COM3' on Windows, '/dev/ttyUSB0' or '/dev/ttyACM0' on Linux/Mac
let SERIAL_PORT = process.env.SERIAL_PORT || null;

// ─── HTTP SERVER (serves the webpage) ────────────────────────────────────────
const httpServer = http.createServer((req, res) => {
  let filePath = path.join(__dirname, req.url === '/' ? 'vigs.html' : req.url);
  const ext = path.extname(filePath);
  const mimeTypes = {
    '.html': 'text/html',
    '.js': 'application/javascript',
    '.css': 'text/css',
  };

  fs.readFile(filePath, (err, data) => {
    if (err) {
      res.writeHead(404);
      res.end('Not found');
      return;
    }
    res.writeHead(200, { 'Content-Type': mimeTypes[ext] || 'text/plain' });
    res.end(data);
  });
});

httpServer.listen(HTTP_PORT, () => {
  console.log(`\n╔══════════════════════════════════════════════╗`);
  console.log(`║       VIGS — Serial Bridge Server            ║`);
  console.log(`╠══════════════════════════════════════════════╣`);
  console.log(`║  Web UI  →  http://localhost:${HTTP_PORT}           ║`);
  console.log(`║  WebSocket →  ws://localhost:${WS_PORT}            ║`);
  console.log(`╚══════════════════════════════════════════════╝\n`);
});

// ─── WEBSOCKET SERVER ─────────────────────────────────────────────────────────
const wss = new WebSocket.Server({ port: WS_PORT });

function broadcast(data) {
  const msg = typeof data === 'string' ? data : JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(msg);
    }
  });
}

wss.on('connection', (ws) => {
  console.log('[WS] Browser connected');
  ws.send(JSON.stringify({ type: 'server_status', status: 'connected', port: currentPort || 'searching...' }));

  // Send current serial port status to newly connected browser
  if (serialConnected) {
    ws.send(JSON.stringify({ type: 'serial_status', connected: true, port: currentPort }));
  } else {
    ws.send(JSON.stringify({ type: 'serial_status', connected: false }));
  }

  ws.on('close', () => console.log('[WS] Browser disconnected'));
});

// ─── SERIAL PORT AUTO-DETECT ─────────────────────────────────────────────────
let serialPort = null;
let serialConnected = false;
let currentPort = null;
let retryTimeout = null;

async function detectArduinoPort() {
  try {
    const ports = await SerialPort.list();
    console.log('[Serial] Available ports:');
    ports.forEach(p => console.log(`  ${p.path} — ${p.manufacturer || 'unknown'}`));

    // Common Arduino identifiers
    const arduinoPort = ports.find(p =>
      (p.manufacturer && (
        p.manufacturer.toLowerCase().includes('arduino') ||
        p.manufacturer.toLowerCase().includes('ch340') ||
        p.manufacturer.toLowerCase().includes('ftdi') ||
        p.manufacturer.toLowerCase().includes('cp210')
      )) ||
      (p.vendorId && ['2341','1a86','0403','10c4'].includes(p.vendorId.toLowerCase())) ||
      (p.path && (p.path.includes('ttyUSB') || p.path.includes('ttyACM')))
    );

    if (arduinoPort) {
      console.log(`[Serial] Arduino detected on: ${arduinoPort.path}`);
      return arduinoPort.path;
    }

    // Fallback: return first available USB port
    const usbPort = ports.find(p => p.path.includes('USB') || p.path.includes('ACM') || p.path.includes('COM'));
    if (usbPort) {
      console.log(`[Serial] Using first available port: ${usbPort.path}`);
      return usbPort.path;
    }

    return null;
  } catch (err) {
    console.error('[Serial] Error listing ports:', err.message);
    return null;
  }
}

async function connectSerial() {
  const portPath = SERIAL_PORT || await detectArduinoPort();

  if (!portPath) {
    console.log('[Serial] No Arduino found. Retrying in 3 seconds...');
    broadcast({ type: 'serial_status', connected: false, message: 'No Arduino detected. Plug in your Arduino...' });
    retryTimeout = setTimeout(connectSerial, 3000);
    return;
  }

  try {
    serialPort = new SerialPort({
      path: portPath,
      baudRate: BAUD_RATE,
      autoOpen: false,
    });

    const parser = serialPort.pipe(new ReadlineParser({ delimiter: '\r\n' }));

    serialPort.open((err) => {
      if (err) {
        console.error(`[Serial] Failed to open ${portPath}:`, err.message);
        broadcast({ type: 'serial_status', connected: false, message: `Cannot open ${portPath}: ${err.message}` });
        retryTimeout = setTimeout(connectSerial, 3000);
        return;
      }

      currentPort = portPath;
      serialConnected = true;
      console.log(`[Serial] Connected to ${portPath} @ ${BAUD_RATE} baud`);
      broadcast({ type: 'serial_status', connected: true, port: portPath });
    });

    parser.on('data', (line) => {
      const trimmed = line.trim();
      if (!trimmed) return;

      console.log(`[Arduino] ${trimmed}`);

      // Broadcast raw line for the terminal
      broadcast({ type: 'serial_data', raw: trimmed, timestamp: Date.now() });

      // Also parse and broadcast structured data
      const parsed = parseLine(trimmed);
      if (parsed) broadcast({ type: 'parsed_data', ...parsed, timestamp: Date.now() });
    });

    serialPort.on('error', (err) => {
      console.error('[Serial] Error:', err.message);
      broadcast({ type: 'serial_status', connected: false, message: err.message });
      handleDisconnect();
    });

    serialPort.on('close', () => {
      console.log('[Serial] Port closed.');
      broadcast({ type: 'serial_status', connected: false, message: 'Arduino disconnected.' });
      handleDisconnect();
    });

  } catch (err) {
    console.error('[Serial] Exception:', err.message);
    retryTimeout = setTimeout(connectSerial, 3000);
  }
}

function handleDisconnect() {
  serialConnected = false;
  currentPort = null;
  serialPort = null;
  if (retryTimeout) clearTimeout(retryTimeout);
  console.log('[Serial] Attempting reconnect in 3s...');
  retryTimeout = setTimeout(connectSerial, 3000);
}

// ─── LINE PARSER ─────────────────────────────────────────────────────────────
function parseLine(line) {
  // Obstacle: 34
  const obstacleMatch = line.match(/^Obstacle:\s*(\d+)/i);
  if (obstacleMatch) {
    return { event: 'obstacle', distance: parseInt(obstacleMatch[1]) };
  }

  // TAG ID: B3 D7 64 56
  const rfidMatch = line.match(/^TAG ID:\s*([A-F0-9 ]+)/i);
  if (rfidMatch) {
    return { event: 'rfid', uid: rfidMatch[1].trim().toUpperCase() };
  }

  // FALL ALERT! Magnitude: 38.45
  const fallMatch = line.match(/^FALL ALERT.*Magnitude:\s*([\d.]+)/i);
  if (fallMatch) {
    return { event: 'fall', magnitude: parseFloat(fallMatch[1]) };
  }

  // System messages
  if (line.includes('SYSTEM') || line.includes('ONLINE') || line.includes('READY') || line.includes('Error')) {
    return { event: 'system', message: line };
  }

  return null;
}

// ─── START ───────────────────────────────────────────────────────────────────
connectSerial();

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n[Server] Shutting down...');
  if (serialPort && serialPort.isOpen) serialPort.close();
  wss.close();
  httpServer.close();
  process.exit(0);
});