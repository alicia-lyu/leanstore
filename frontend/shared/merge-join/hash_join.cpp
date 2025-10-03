#include "hash_join.hpp"
#include "join_state.hpp"

const static LoggerFlusher<HashLogger> final_flusher; // program scope