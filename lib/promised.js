module.exports = promised

async function promised (action) {
  return new Promise((resolve, reject) => {
    action((error, result) => error ? reject(error) : resolve(result))
  })
}
