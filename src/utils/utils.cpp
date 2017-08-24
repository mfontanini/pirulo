#include <random>
#include <array>
#include "utils/utils.h"

using std::string;
using std::array;
using std::random_device;
using std::mt19937;
using std::uniform_int_distribution;

namespace pirulo {
namespace utils {

static const size_t GROUP_ID_LENGTH = 16;

string generate_group_id() {
    array<char, 16> alphanums;
    for (size_t i = 0; i <= 9; ++i) {
        alphanums[i] = '0' + i;
    }
    for (size_t c = 'a'; c <= 'f'; ++c) {
        alphanums[10 + c - 'a'] = c;
    }
    const auto seed = random_device{}();
    mt19937 generator(seed);
    uniform_int_distribution<size_t> distribution(0, alphanums.size() - 1);

    string output = "pirulo-";
    for (size_t i = 0; i < GROUP_ID_LENGTH; ++i) {
        output.push_back(alphanums[distribution(generator)]);
    }
    return output;
}

} // utils
} // pirulo

