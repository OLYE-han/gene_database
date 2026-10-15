#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#include <Python.h>
#include <numpy/arrayobject.h>

#include <cstdint>
#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#endif
#include <vector>

static PyObject* packed_sign_dot(PyObject*, PyObject* args) {
    PyObject* codes_object = nullptr;
    PyObject* query_object = nullptr;
    int dimension = 0;
    if (!PyArg_ParseTuple(args, "OOi", &codes_object, &query_object, &dimension)) {
        return nullptr;
    }

    PyArrayObject* codes = reinterpret_cast<PyArrayObject*>(
        PyArray_FROM_OTF(codes_object, NPY_UINT8, NPY_ARRAY_IN_ARRAY));
    PyArrayObject* query = reinterpret_cast<PyArrayObject*>(
        PyArray_FROM_OTF(query_object, NPY_FLOAT32, NPY_ARRAY_IN_ARRAY));
    if (codes == nullptr || query == nullptr) {
        Py_XDECREF(codes);
        Py_XDECREF(query);
        return nullptr;
    }
    if (PyArray_NDIM(codes) != 2 || PyArray_NDIM(query) != 1) {
        PyErr_SetString(PyExc_ValueError, "codes must be 2D and query must be 1D");
        Py_DECREF(codes);
        Py_DECREF(query);
        return nullptr;
    }

    const npy_intp rows = PyArray_DIM(codes, 0);
    const npy_intp width = PyArray_DIM(codes, 1);
    if (dimension <= 0 || dimension > width * 8 || PyArray_DIM(query, 0) < dimension) {
        PyErr_SetString(PyExc_ValueError, "dimension does not match codes/query");
        Py_DECREF(codes);
        Py_DECREF(query);
        return nullptr;
    }

    npy_intp output_shape[1] = {rows};
    PyArrayObject* output = reinterpret_cast<PyArrayObject*>(
        PyArray_SimpleNew(1, output_shape, NPY_FLOAT32));
    if (output == nullptr) {
        Py_DECREF(codes);
        Py_DECREF(query);
        return nullptr;
    }

    const auto* code_data = static_cast<const std::uint8_t*>(PyArray_DATA(codes));
    const auto* query_data = static_cast<const float*>(PyArray_DATA(query));
    auto* output_data = static_cast<float*>(PyArray_DATA(output));
    std::vector<float> lookup(static_cast<std::size_t>(width) * 256);

    for (npy_intp byte_index = 0; byte_index < width; ++byte_index) {
        for (int value = 0; value < 256; ++value) {
            float sum = 0.0F;
            for (int bit = 0; bit < 8; ++bit) {
                const int coordinate = static_cast<int>(byte_index * 8 + bit);
                if (coordinate >= dimension) {
                    break;
                }
                const float sign = (value & (1 << bit)) ? 1.0F : -1.0F;
                sum += sign * query_data[coordinate];
            }
            lookup[static_cast<std::size_t>(byte_index) * 256 + value] = sum;
        }
    }

    Py_BEGIN_ALLOW_THREADS
    for (npy_intp row = 0; row < rows; ++row) {
        const auto* row_codes = code_data + row * width;
        float sum = 0.0F;
        for (npy_intp byte_index = 0; byte_index < width; ++byte_index) {
            sum += lookup[static_cast<std::size_t>(byte_index) * 256 + row_codes[byte_index]];
        }
        output_data[row] = sum;
    }
    Py_END_ALLOW_THREADS

    Py_DECREF(codes);
    Py_DECREF(query);
    return reinterpret_cast<PyObject*>(output);
}

static inline float scalar_fastscan_row(
    const std::uint8_t* codes,
    npy_intp width,
    npy_intp block,
    npy_intp lane,
    const float* low_lookup,
    const float* high_lookup) {
    float sum = 0.0F;
    for (npy_intp byte_index = 0; byte_index < width; ++byte_index) {
        const std::uint8_t value = codes[(block * width + byte_index) * 32 + lane];
        sum += low_lookup[byte_index * 16 + (value & 15)];
        sum += high_lookup[byte_index * 16 + (value >> 4)];
    }
    return sum;
}

#if defined(__GNUC__) && (defined(__x86_64__) || defined(_M_X64))
__attribute__((target("avx512f,avx512bw")))
static void avx512_fastscan_rows(
    const std::uint8_t* codes,
    npy_intp width,
    npy_intp block,
    npy_intp lane,
    const float* low_lookup,
    const float* high_lookup,
    float* output) {
    __m512 sums = _mm512_setzero_ps();
    const __m512i mask = _mm512_set1_epi32(15);
    for (npy_intp byte_index = 0; byte_index < width; ++byte_index) {
        const auto* source = codes + (block * width + byte_index) * 32 + lane;
        const __m128i bytes = _mm_loadu_si128(reinterpret_cast<const __m128i*>(source));
        const __m512i values = _mm512_cvtepu8_epi32(bytes);
        const __m512i low = _mm512_and_si512(values, mask);
        const __m512i high = _mm512_srli_epi32(values, 4);
        const __m512 low_table = _mm512_loadu_ps(low_lookup + byte_index * 16);
        const __m512 high_table = _mm512_loadu_ps(high_lookup + byte_index * 16);
        sums = _mm512_add_ps(sums, _mm512_permutexvar_ps(low, low_table));
        sums = _mm512_add_ps(sums, _mm512_permutexvar_ps(high, high_table));
    }
    _mm512_storeu_ps(output, sums);
}

__attribute__((target("avx2")))
static void avx2_fastscan_rows(
    const std::uint8_t* codes,
    npy_intp width,
    npy_intp block,
    npy_intp lane,
    const float* lookup,
    float* output) {
    __m256 sums = _mm256_setzero_ps();
    for (npy_intp byte_index = 0; byte_index < width; ++byte_index) {
        const auto* source = codes + (block * width + byte_index) * 32 + lane;
        const __m128i bytes = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(source));
        const __m256i values = _mm256_cvtepu8_epi32(bytes);
        const float* table = lookup + byte_index * 256;
        sums = _mm256_add_ps(sums, _mm256_i32gather_ps(table, values, 4));
    }
    _mm256_storeu_ps(output, sums);
}
#endif

static PyObject* packed_sign_dot_fastscan(PyObject*, PyObject* args) {
    PyObject* codes_object = nullptr;
    PyObject* query_object = nullptr;
    int dimension = 0;
    long long begin = 0;
    long long end = 0;
    if (!PyArg_ParseTuple(
            args, "OOiLL", &codes_object, &query_object, &dimension, &begin, &end)) {
        return nullptr;
    }
    PyArrayObject* codes = reinterpret_cast<PyArrayObject*>(
        PyArray_FROM_OTF(codes_object, NPY_UINT8, NPY_ARRAY_IN_ARRAY));
    PyArrayObject* query = reinterpret_cast<PyArrayObject*>(
        PyArray_FROM_OTF(query_object, NPY_FLOAT32, NPY_ARRAY_IN_ARRAY));
    if (codes == nullptr || query == nullptr) {
        Py_XDECREF(codes);
        Py_XDECREF(query);
        return nullptr;
    }
    if (PyArray_NDIM(codes) != 3 || PyArray_DIM(codes, 2) != 32 ||
        PyArray_NDIM(query) != 1) {
        PyErr_SetString(PyExc_ValueError, "FastScan codes must have shape (blocks, width, 32)");
        Py_DECREF(codes);
        Py_DECREF(query);
        return nullptr;
    }
    const npy_intp blocks = PyArray_DIM(codes, 0);
    const npy_intp width = PyArray_DIM(codes, 1);
    const long long capacity = static_cast<long long>(blocks * 32);
    if (dimension <= 0 || dimension > width * 8 || begin < 0 || end < begin ||
        end > capacity || PyArray_DIM(query, 0) < dimension) {
        PyErr_SetString(PyExc_ValueError, "invalid FastScan range or dimension");
        Py_DECREF(codes);
        Py_DECREF(query);
        return nullptr;
    }

    npy_intp output_shape[1] = {static_cast<npy_intp>(end - begin)};
    PyArrayObject* output = reinterpret_cast<PyArrayObject*>(
        PyArray_SimpleNew(1, output_shape, NPY_FLOAT32));
    if (output == nullptr) {
        Py_DECREF(codes);
        Py_DECREF(query);
        return nullptr;
    }
    const auto* code_data = static_cast<const std::uint8_t*>(PyArray_DATA(codes));
    const auto* query_data = static_cast<const float*>(PyArray_DATA(query));
    auto* output_data = static_cast<float*>(PyArray_DATA(output));
    std::vector<float> low_lookup(static_cast<std::size_t>(width) * 16);
    std::vector<float> high_lookup(static_cast<std::size_t>(width) * 16);
    for (npy_intp byte_index = 0; byte_index < width; ++byte_index) {
        for (int value = 0; value < 16; ++value) {
            float low_sum = 0.0F;
            float high_sum = 0.0F;
            for (int bit = 0; bit < 4; ++bit) {
                const int low_coordinate = static_cast<int>(byte_index * 8 + bit);
                const int high_coordinate = low_coordinate + 4;
                const float sign = (value & (1 << bit)) ? 1.0F : -1.0F;
                if (low_coordinate < dimension) {
                    low_sum += sign * query_data[low_coordinate];
                }
                if (high_coordinate < dimension) {
                    high_sum += sign * query_data[high_coordinate];
                }
            }
            low_lookup[byte_index * 16 + value] = low_sum;
            high_lookup[byte_index * 16 + value] = high_sum;
        }
    }

    bool use_avx512 = false;
    bool use_avx2 = false;
#if defined(__GNUC__) && (defined(__x86_64__) || defined(_M_X64))
    __builtin_cpu_init();
    use_avx512 = __builtin_cpu_supports("avx512f") &&
                 __builtin_cpu_supports("avx512bw");
    use_avx2 = !use_avx512 && __builtin_cpu_supports("avx2");
#endif
    std::vector<float> avx2_lookup;
    if (use_avx2) {
        avx2_lookup.resize(static_cast<std::size_t>(width) * 256);
        for (npy_intp byte_index = 0; byte_index < width; ++byte_index) {
            for (int value = 0; value < 256; ++value) {
                avx2_lookup[static_cast<std::size_t>(byte_index) * 256 + value] =
                    low_lookup[byte_index * 16 + (value & 15)] +
                    high_lookup[byte_index * 16 + (value >> 4)];
            }
        }
    }
    long long row = begin;
    Py_BEGIN_ALLOW_THREADS
    while (row < end) {
        const npy_intp block = static_cast<npy_intp>(row / 32);
        const npy_intp lane = static_cast<npy_intp>(row % 32);
#if defined(__GNUC__) && (defined(__x86_64__) || defined(_M_X64))
        if (use_avx512 && lane % 16 == 0 && lane <= 16 && row + 16 <= end) {
            avx512_fastscan_rows(
                code_data, width, block, lane, low_lookup.data(), high_lookup.data(),
                output_data + (row - begin));
            row += 16;
            continue;
        }
        if (use_avx2 && lane % 8 == 0 && lane <= 24 && row + 8 <= end) {
            avx2_fastscan_rows(
                code_data, width, block, lane, avx2_lookup.data(),
                output_data + (row - begin));
            row += 8;
            continue;
        }
#endif
        output_data[row - begin] = scalar_fastscan_row(
            code_data, width, block, lane, low_lookup.data(), high_lookup.data());
        ++row;
    }
    Py_END_ALLOW_THREADS

    Py_DECREF(codes);
    Py_DECREF(query);
    return reinterpret_cast<PyObject*>(output);
}

static PyObject* fastscan_backend(PyObject*, PyObject*) {
#if defined(__GNUC__) && (defined(__x86_64__) || defined(_M_X64))
    __builtin_cpu_init();
    if (__builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512bw")) {
        return PyUnicode_FromString("avx512");
    }
    if (__builtin_cpu_supports("avx2")) {
        return PyUnicode_FromString("avx2");
    }
#endif
    return PyUnicode_FromString("scalar");
}

static PyMethodDef methods[] = {
    {"packed_sign_dot", packed_sign_dot, METH_VARARGS,
     "Compute weighted dot products for packed {-1,+1} codes."},
    {"packed_sign_dot_fastscan", packed_sign_dot_fastscan, METH_VARARGS,
     "Scan block-transposed RaBitQ codes with SIMD nibble LUTs."},
    {"fastscan_backend", fastscan_backend, METH_NOARGS,
     "Return the SIMD backend selected for FastScan."},
    {nullptr, nullptr, 0, nullptr},
};

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    "_native",
    "Native RaBitQ scanning primitives.",
    -1,
    methods,
};

PyMODINIT_FUNC PyInit__native(void) {
    import_array();
    return PyModule_Create(&module);
}
