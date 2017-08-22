var mongoose = require('mongoose');
var airline_dep_delay = new mongoose.Schema({
  airline: String,
  origin: String,
  delay: Number,
  updated_at: { type: Date, default: Date.now },
});

module.exports = mongoose.model('AirlineDepDelay', airline_dep_delay);
