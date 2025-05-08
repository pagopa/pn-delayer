function chunkArray(messages, size) {
    return Array.from({ length: Math.ceil(messages.length / size) },
    (_, i) => messages.slice(i * size, i * size + size));
}

module.exports = { chunkArray };