const mongoose = require("mongoose");

// Receipt metadata survives encrypted-payload deletion so senders can catch up
// after reconnecting. No message content or keys are retained here.
const schema = new mongoose.Schema({
  messageId: { type: String, required: true, unique: true },
  clientMessageId: { type: String, default: null },
  senderChatId: { type: String, required: true, index: true },
  receiverChatId: { type: String, required: true },
  status: { type: Number, enum: [1, 2], required: true }, // delivered, read
  expiresAt: { type: Date, required: true, expires: 0 },
});

module.exports = mongoose.model("MessageReceipt", schema);
