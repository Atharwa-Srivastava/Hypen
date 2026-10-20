const normalize = (value) => String(value || "").trim().replace(/^@+/, "");
const aliases = (value) => [normalize(value), `@${normalize(value)}`];
const userRoom = (value) => `user:${normalize(value)}`;

function broadcastMessage(io, receiver, sender, payload, groupId) {
  // A single union broadcast reaches each socket exactly once. Group messages
  // already fan out per recipient through /send; never broadcast those payloads
  // to the entire group again.
  const rooms = [userRoom(receiver)];
  if (!groupId && normalize(sender)) rooms.push(userRoom(sender));
  io.to(rooms).emit("newMessage", payload);
}

async function isPresent(io, chatId) {
  const sockets = await io.in(userRoom(chatId)).fetchSockets();
  return sockets.some((socket) => socket.data.foreground !== false);
}

function createReceiptService({ io, Message, MessageReceipt, User }) {
  async function record(messageId, readerId, status) {
    const reader = normalize(readerId);
    const lookup = {
      $or: [{ messageId }, { clientMessageId: messageId }],
      receiverChatId: { $in: aliases(reader) },
    };
    const message = await Message.findOne(lookup).lean() ||
      await MessageReceipt.findOne(lookup).lean();
    if (!message || message.groupId || message.ghostSessionId ||
        normalize(message.senderChatId) === reader) return null;

    if (status === 2) {
      const users = await User.find({
        chatId: { $in: [...aliases(reader), ...aliases(message.senderChatId)] },
      }).select("privacy").lean();
      if (users.some((user) => user.privacy?.readReceipts === false)) return null;
    }

    const update = {
      $setOnInsert: {
        messageId: message.messageId,
        clientMessageId: message.clientMessageId || null,
        senderChatId: normalize(message.senderChatId),
        receiverChatId: reader,
      },
      $max: { status },
      $set: { expiresAt: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000) },
    };
    const options = { upsert: true, new: true };
    let receipt;
    try {
      receipt = await MessageReceipt.findOneAndUpdate(
        { messageId: message.messageId }, update, options,
      ).lean();
    } catch (error) {
      // A read and delivery ACK can arrive together on different workers.
      if (error.code !== 11000) throw error;
      receipt = await MessageReceipt.findOneAndUpdate(
        { messageId: message.messageId }, update, { new: true },
      ).lean();
    }
    await Message.updateOne(
      { messageId: message.messageId, status: { $ne: "read" } },
      { $set: { status: receipt.status === 2 ? "read" : "delivered" } },
    );
    const payload = toEvent(receipt);
    io.to(userRoom(receipt.senderChatId)).emit(payload.type, payload);
    return receipt;
  }

  function toEvent(receipt) {
    return {
      type: receipt.status === 2 ? "message_read" : "message_delivered",
      messageId: receipt.messageId,
      clientMessageId: receipt.clientMessageId,
      from: receipt.receiverChatId,
      to: receipt.senderChatId,
    };
  }

  async function sync(sender, messageIds) {
    const receipts = await MessageReceipt.find({
      senderChatId: normalize(sender),
      $or: [
        { messageId: { $in: messageIds } },
        { clientMessageId: { $in: messageIds } },
      ],
      expiresAt: { $gt: new Date() },
    }).lean();
    return receipts.map(toEvent);
  }

  return { record, sync };
}

module.exports = { broadcastMessage, isPresent, createReceiptService };
