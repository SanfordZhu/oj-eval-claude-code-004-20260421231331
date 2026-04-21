// Bookstore Management System
// ACMOJ 1075 / 1775
//
// File-based implementation using B+ trees for indexing. Account and book
// records live in fixed-size record files; indexes link queryable fields to
// record ids. Financial transactions are kept in a sequential log file.

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>
#include <cmath>
#include <fstream>

#include "bptree.hpp"
#include "store.hpp"
#include "keys.hpp"

// --------------------- Record structures ---------------------

struct UserRec {
    char user_id[32];
    char password[32];
    char username[32];
    int8_t privilege;
};

struct BookRec {
    char isbn[24];
    char name[64];
    char author[64];
    char keyword[64]; // full raw keyword string (with | separators)
    double price;
    int64_t stock;
};

struct FinanceRec {
    double income;
    double expenditure;
};

// --------------------- Utility ---------------------

static inline bool is_valid_userid_char(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
           (c >= '0' && c <= '9') || c == '_';
}

// ISBN / BookName / Author / Keyword: printable ASCII, no quotes
static inline bool is_visible_no_quote(char c) {
    if (c == '"') return false;
    return (unsigned char)c >= 32 && (unsigned char)c < 127;
}

static inline bool is_visible(char c) {
    return (unsigned char)c >= 32 && (unsigned char)c < 127;
}

static inline bool is_digit_char(char c) {
    return c >= '0' && c <= '9';
}

static bool valid_userid(const std::string &s, int maxlen = 30) {
    if (s.empty() || (int)s.size() > maxlen) return false;
    for (char c : s) if (!is_valid_userid_char(c)) return false;
    return true;
}

static bool valid_username(const std::string &s) {
    if (s.empty() || (int)s.size() > 30) return false;
    for (char c : s) if (!is_visible(c)) return false;
    return true;
}

static bool valid_isbn(const std::string &s) {
    if (s.empty() || (int)s.size() > 20) return false;
    for (char c : s) if (!is_visible_no_quote(c)) return false;
    return true;
}

static bool valid_bookstr(const std::string &s) {
    // name, author
    if (s.empty() || (int)s.size() > 60) return false;
    for (char c : s) if (!is_visible_no_quote(c)) return false;
    return true;
}

static bool parse_uint32(const std::string &s, int &v) {
    if (s.empty() || s.size() > 10) return false;
    for (char c : s) if (!is_digit_char(c)) return false;
    long long x = 0;
    for (char c : s) {
        x = x * 10 + (c - '0');
        if (x > 2147483647LL) return false;
    }
    v = (int)x;
    return true;
}

static bool parse_price(const std::string &s, double &v) {
    if (s.empty() || s.size() > 13) return false;
    int dotcount = 0;
    for (char c : s) {
        if (c == '.') { dotcount++; if (dotcount > 1) return false; }
        else if (!is_digit_char(c)) return false;
    }
    // At least one digit
    bool has_digit = false;
    for (char c : s) if (is_digit_char(c)) { has_digit = true; break; }
    if (!has_digit) return false;
    try {
        v = std::stod(s);
    } catch (...) { return false; }
    return true;
}

static bool valid_privilege(const std::string &s) {
    if (s.size() != 1) return false;
    return s[0] == '1' || s[0] == '3' || s[0] == '7';
}

static bool parse_keyword_list(const std::string &s, std::vector<std::string> &out, bool allow_multiple = true) {
    out.clear();
    if (s.empty()) return false;
    // split by |
    std::string cur;
    for (char c : s) {
        if (!is_visible_no_quote(c)) return false;
        if (c == '|') {
            if (cur.empty()) return false;
            out.push_back(cur);
            cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    if (cur.empty()) return false;
    out.push_back(cur);
    if (!allow_multiple && out.size() != 1) return false;
    // total length check is implied by 60
    if (s.size() > 60) return false;
    return true;
}

// --------------------- Global state ---------------------

struct LoginFrame {
    int32_t user_rec_id;
    char user_id[32];
    int8_t privilege;
    int32_t selected_book_id; // -1 if none
};

std::vector<LoginFrame> login_stack;

BPTree<UserKey> user_index;
BPTree<ISBNKey> isbn_index;
BPTree<NameKey> name_index;
BPTree<AuthorKey> author_index;
BPTree<KeywordKey> keyword_index;

RecordStore<UserRec> user_store;
RecordStore<BookRec> book_store;

// Finance log - append only sequential file
std::fstream finance_file;
int32_t finance_count = 0;

// Helpers for record field -> string
static std::string to_str_field(const char *buf, int maxlen) {
    int len = 0;
    while (len < maxlen && buf[len] != 0) len++;
    return std::string(buf, len);
}

static void set_str_field(char *buf, int maxlen, const std::string &s) {
    memset(buf, 0, maxlen);
    int len = (int)s.size();
    if (len > maxlen) len = maxlen;
    memcpy(buf, s.c_str(), len);
}

// Output a price formatted to 2 decimals
static void print_price(double v) {
    char buf[64];
    // Avoid negative zero
    if (v < 0 && v > -0.005) v = 0;
    snprintf(buf, sizeof(buf), "%.2f", v);
    std::cout << buf;
}

// --------------------- Finance ---------------------

static void finance_open(const std::string &fname) {
    finance_file.open(fname, std::ios::in | std::ios::out | std::ios::binary);
    if (!finance_file.is_open()) {
        finance_file.clear();
        finance_file.open(fname, std::ios::out | std::ios::binary);
        finance_file.close();
        finance_file.open(fname, std::ios::in | std::ios::out | std::ios::binary);
        finance_count = 0;
        finance_file.seekp(0);
        finance_file.write(reinterpret_cast<char *>(&finance_count), sizeof(int32_t));
    } else {
        finance_file.seekg(0);
        finance_file.read(reinterpret_cast<char *>(&finance_count), sizeof(int32_t));
    }
}

static void finance_append(double income, double expenditure) {
    FinanceRec r{income, expenditure};
    finance_file.seekp(sizeof(int32_t) + (std::streamoff)finance_count * sizeof(FinanceRec));
    finance_file.write(reinterpret_cast<const char *>(&r), sizeof(FinanceRec));
    finance_count++;
    finance_file.seekp(0);
    finance_file.write(reinterpret_cast<const char *>(&finance_count), sizeof(int32_t));
}

static void finance_get_sum(int n, double &inc, double &exp) {
    inc = 0;
    exp = 0;
    int start = finance_count - n;
    FinanceRec r;
    for (int i = start; i < finance_count; i++) {
        finance_file.seekg(sizeof(int32_t) + (std::streamoff)i * sizeof(FinanceRec));
        finance_file.read(reinterpret_cast<char *>(&r), sizeof(FinanceRec));
        inc += r.income;
        exp += r.expenditure;
    }
}

// --------------------- Init ---------------------

static bool file_exists(const std::string &fname) {
    std::ifstream f(fname);
    return f.good();
}

static void init_storage() {
    bool first_run = !file_exists("bookstore_user.dat");
    user_store.open("bookstore_user.dat");
    book_store.open("bookstore_book.dat");
    user_index.open("bookstore_user_idx.dat");
    isbn_index.open("bookstore_isbn_idx.dat");
    name_index.open("bookstore_name_idx.dat");
    author_index.open("bookstore_author_idx.dat");
    keyword_index.open("bookstore_keyword_idx.dat");
    finance_open("bookstore_finance.dat");

    if (first_run) {
        UserRec u;
        set_str_field(u.user_id, 32, "root");
        set_str_field(u.password, 32, "sjtu");
        set_str_field(u.username, 32, "root");
        u.privilege = 7;
        int32_t id = user_store.append(u);
        user_index.insert(UserKey("root"), id);
    }
}

static void close_storage() {
    user_store.close();
    book_store.close();
    user_index.close();
    isbn_index.close();
    name_index.close();
    author_index.close();
    keyword_index.close();
    finance_file.close();
}

// --------------------- Login helpers ---------------------

static int current_priv() {
    if (login_stack.empty()) return 0;
    return login_stack.back().privilege;
}

static int32_t find_user(const std::string &userid) {
    auto v = user_index.find(UserKey(userid));
    if (v.empty()) return -1;
    return v[0];
}

static bool is_logged_in(const std::string &userid) {
    for (auto &f : login_stack) {
        if (to_str_field(f.user_id, 32) == userid) return true;
    }
    return false;
}

// --------------------- Command parsing ---------------------

static std::vector<std::string> split_tokens(const std::string &s) {
    std::vector<std::string> out;
    int i = 0, n = s.size();
    while (i < n) {
        while (i < n && s[i] == ' ') i++;
        if (i >= n) break;
        int j = i;
        while (j < n && s[j] != ' ') j++;
        out.push_back(s.substr(i, j - i));
        i = j;
    }
    return out;
}

// Parse a quoted arg value. Returns the content between quotes. Fails if format wrong.
// Used only in 'show' and 'modify' - we get a token like -name="Foo Bar" but tokens are split by space.
// So we need to parse the WHOLE command more carefully for quoted strings with spaces.

// We'll implement a smarter tokenizer that respects spaces only outside quoted sections.
// Spec says book names/keywords/authors cannot contain spaces (visible chars except quote & invisible).
// Actually: "Legal character set: ASCII characters except invisible characters and English double quotes"
// Space IS visible (ASCII 32). So book names CAN have spaces? Wait - "invisible" usually means non-printable.
// Space is printable... Looking at example outputs, names have no spaces typically.
// But the command format uses -name="[BookName]" which suggests quotes - so name CAN include spaces.
// However the top-level command parsing says split by spaces. So we need special handling.
//
// Let's interpret: "Only space is allowed as whitespace; a single command is split into multiple parts by spaces"
// But for quoted book names with spaces, we can't split. So the convention is:
// The name within "..." may contain spaces (since the quotes delimit them).
//
// To be safe I'll tokenize respecting the fact that after -name=, -author=, -keyword=, text up to the NEXT
// unmatched " followed by end-of-token is kept together.

// Simpler: for -name="..." form, the text between quotes is the value. Outside,
// still split by spaces. For the `-ISBN=...` and `-price=...` forms there's no quote.
// So we tokenize: first split by spaces (treating multiple spaces as one), but if a
// token begins with -key=" we keep appending until we find a token ending with ".

static std::vector<std::string> smart_tokens(const std::string &s) {
    std::vector<std::string> raw = split_tokens(s);
    // Post-process: merge tokens that are inside unfinished quotes.
    std::vector<std::string> out;
    int i = 0;
    while (i < (int)raw.size()) {
        const std::string &t = raw[i];
        // Check if it contains a quote character
        bool has_quote = false;
        for (char c : t) if (c == '"') { has_quote = true; break; }
        if (!has_quote) {
            out.push_back(t);
            i++;
            continue;
        }
        // Count quotes; if even, token is self-contained
        int qcount = 0;
        for (char c : t) if (c == '"') qcount++;
        if (qcount % 2 == 0) {
            out.push_back(t);
            i++;
            continue;
        }
        // Need to merge with following tokens
        std::string acc = t;
        i++;
        bool ok = false;
        while (i < (int)raw.size()) {
            acc.push_back(' ');
            acc += raw[i];
            int q2 = 0;
            for (char c : raw[i]) if (c == '"') q2++;
            qcount += q2;
            i++;
            if (qcount % 2 == 0) { ok = true; break; }
        }
        (void)ok;
        out.push_back(acc);
    }
    return out;
}

// Extract a quoted value from a token like -name="value"
// Returns true on success.
static bool extract_quoted(const std::string &tok, const std::string &prefix, std::string &out) {
    // prefix should end with '='
    if (tok.size() < prefix.size() + 2) return false;
    if (tok.substr(0, prefix.size()) != prefix) return false;
    if (tok[prefix.size()] != '"') return false;
    if (tok.back() != '"') return false;
    out = tok.substr(prefix.size() + 1, tok.size() - prefix.size() - 2);
    return true;
}

static bool extract_plain(const std::string &tok, const std::string &prefix, std::string &out) {
    if (tok.size() <= prefix.size()) return false;
    if (tok.substr(0, prefix.size()) != prefix) return false;
    out = tok.substr(prefix.size());
    return true;
}

// --------------------- Commands ---------------------

static bool fail() {
    std::cout << "Invalid\n";
    return false;
}

static void cmd_su(const std::vector<std::string> &args) {
    if (args.size() != 2 && args.size() != 3) { fail(); return; }
    const std::string &uid = args[1];
    if (!valid_userid(uid)) { fail(); return; }
    int32_t rid = find_user(uid);
    if (rid == -1) { fail(); return; }
    UserRec u = user_store.read(rid);
    if (args.size() == 3) {
        if (!valid_userid(args[2])) { fail(); return; }
        if (to_str_field(u.password, 32) != args[2]) { fail(); return; }
    } else {
        if (current_priv() <= u.privilege) { fail(); return; }
    }
    LoginFrame f;
    f.user_rec_id = rid;
    memcpy(f.user_id, u.user_id, 32);
    f.privilege = u.privilege;
    f.selected_book_id = -1;
    login_stack.push_back(f);
}

static void cmd_logout(const std::vector<std::string> &args) {
    if (args.size() != 1) { fail(); return; }
    if (login_stack.empty()) { fail(); return; }
    login_stack.pop_back();
}

static void cmd_register(const std::vector<std::string> &args) {
    if (args.size() != 4) { fail(); return; }
    if (!valid_userid(args[1])) { fail(); return; }
    if (!valid_userid(args[2])) { fail(); return; }
    if (!valid_username(args[3])) { fail(); return; }
    if (find_user(args[1]) != -1) { fail(); return; }
    UserRec u;
    set_str_field(u.user_id, 32, args[1]);
    set_str_field(u.password, 32, args[2]);
    set_str_field(u.username, 32, args[3]);
    u.privilege = 1;
    int32_t id = user_store.append(u);
    user_index.insert(UserKey(args[1]), id);
}

static void cmd_passwd(const std::vector<std::string> &args) {
    if (current_priv() < 1) { fail(); return; }
    if (args.size() != 3 && args.size() != 4) { fail(); return; }
    if (!valid_userid(args[1])) { fail(); return; }
    int32_t rid = find_user(args[1]);
    if (rid == -1) { fail(); return; }
    UserRec u = user_store.read(rid);
    std::string newpw;
    if (args.size() == 4) {
        if (!valid_userid(args[2]) || !valid_userid(args[3])) { fail(); return; }
        if (to_str_field(u.password, 32) != args[2]) { fail(); return; }
        newpw = args[3];
    } else {
        if (current_priv() != 7) { fail(); return; }
        if (!valid_userid(args[2])) { fail(); return; }
        newpw = args[2];
    }
    set_str_field(u.password, 32, newpw);
    user_store.write(rid, u);
}

static void cmd_useradd(const std::vector<std::string> &args) {
    if (current_priv() < 3) { fail(); return; }
    if (args.size() != 5) { fail(); return; }
    if (!valid_userid(args[1])) { fail(); return; }
    if (!valid_userid(args[2])) { fail(); return; }
    if (!valid_privilege(args[3])) { fail(); return; }
    if (!valid_username(args[4])) { fail(); return; }
    int priv = args[3][0] - '0';
    if (priv >= current_priv()) { fail(); return; }
    if (find_user(args[1]) != -1) { fail(); return; }
    UserRec u;
    set_str_field(u.user_id, 32, args[1]);
    set_str_field(u.password, 32, args[2]);
    set_str_field(u.username, 32, args[4]);
    u.privilege = (int8_t)priv;
    int32_t id = user_store.append(u);
    user_index.insert(UserKey(args[1]), id);
}

static void cmd_delete(const std::vector<std::string> &args) {
    if (current_priv() < 7) { fail(); return; }
    if (args.size() != 2) { fail(); return; }
    if (!valid_userid(args[1])) { fail(); return; }
    int32_t rid = find_user(args[1]);
    if (rid == -1) { fail(); return; }
    if (is_logged_in(args[1])) { fail(); return; }
    user_index.erase(UserKey(args[1]), rid);
    user_store.erase(rid);
}

// --------------------- Books ---------------------

static void print_book(const BookRec &b) {
    std::cout << to_str_field(b.isbn, 24) << '\t';
    std::cout << to_str_field(b.name, 64) << '\t';
    std::cout << to_str_field(b.author, 64) << '\t';
    std::cout << to_str_field(b.keyword, 64) << '\t';
    print_price(b.price);
    std::cout << '\t' << b.stock << '\n';
}

static void add_book_indexes(const BookRec &b, int32_t id) {
    isbn_index.insert(ISBNKey(to_str_field(b.isbn, 24)), id);
    std::string name = to_str_field(b.name, 64);
    if (!name.empty()) name_index.insert(NameKey(name), id);
    std::string author = to_str_field(b.author, 64);
    if (!author.empty()) author_index.insert(AuthorKey(author), id);
    std::string kw = to_str_field(b.keyword, 64);
    if (!kw.empty()) {
        std::vector<std::string> kws;
        parse_keyword_list(kw, kws);
        for (auto &k : kws) keyword_index.insert(KeywordKey(k), id);
    }
}

static void remove_book_indexes(const BookRec &b, int32_t id) {
    isbn_index.erase(ISBNKey(to_str_field(b.isbn, 24)), id);
    std::string name = to_str_field(b.name, 64);
    if (!name.empty()) name_index.erase(NameKey(name), id);
    std::string author = to_str_field(b.author, 64);
    if (!author.empty()) author_index.erase(AuthorKey(author), id);
    std::string kw = to_str_field(b.keyword, 64);
    if (!kw.empty()) {
        std::vector<std::string> kws;
        parse_keyword_list(kw, kws);
        for (auto &k : kws) keyword_index.erase(KeywordKey(k), id);
    }
}

static void cmd_show(const std::vector<std::string> &args) {
    if (current_priv() < 1) { fail(); return; }
    // show (no args): print all books
    if (args.size() == 1) {
        // iterate isbn_index in order
        std::vector<int32_t> ids;
        isbn_index.iterate_all([&](const ISBNKey &, int32_t v) { ids.push_back(v); });
        if (ids.empty()) {
            std::cout << '\n';
            return;
        }
        for (int32_t id : ids) {
            BookRec b = book_store.read(id);
            print_book(b);
        }
        return;
    }
    if (args.size() != 2) { fail(); return; }
    const std::string &t = args[1];
    std::vector<int32_t> ids;
    if (t.rfind("-ISBN=", 0) == 0) {
        std::string v = t.substr(6);
        if (!valid_isbn(v)) { fail(); return; }
        ids = isbn_index.find(ISBNKey(v));
    } else if (t.rfind("-name=", 0) == 0) {
        std::string v;
        if (!extract_quoted(t, "-name=", v)) { fail(); return; }
        if (!valid_bookstr(v)) { fail(); return; }
        ids = name_index.find(NameKey(v));
    } else if (t.rfind("-author=", 0) == 0) {
        std::string v;
        if (!extract_quoted(t, "-author=", v)) { fail(); return; }
        if (!valid_bookstr(v)) { fail(); return; }
        ids = author_index.find(AuthorKey(v));
    } else if (t.rfind("-keyword=", 0) == 0) {
        std::string v;
        if (!extract_quoted(t, "-keyword=", v)) { fail(); return; }
        std::vector<std::string> kws;
        if (!parse_keyword_list(v, kws, false)) { fail(); return; } // must be single keyword
        ids = keyword_index.find(KeywordKey(kws[0]));
    } else {
        fail();
        return;
    }
    if (ids.empty()) {
        std::cout << '\n';
        return;
    }
    // Sort by ISBN
    std::vector<BookRec> books;
    books.reserve(ids.size());
    for (int32_t id : ids) books.push_back(book_store.read(id));
    std::sort(books.begin(), books.end(), [](const BookRec &a, const BookRec &b) {
        return memcmp(a.isbn, b.isbn, 24) < 0;
    });
    for (auto &b : books) print_book(b);
}

static void cmd_buy(const std::vector<std::string> &args) {
    if (current_priv() < 1) { fail(); return; }
    if (args.size() != 3) { fail(); return; }
    if (!valid_isbn(args[1])) { fail(); return; }
    int qty;
    if (!parse_uint32(args[2], qty) || qty <= 0) { fail(); return; }
    auto v = isbn_index.find(ISBNKey(args[1]));
    if (v.empty()) { fail(); return; }
    int32_t id = v[0];
    BookRec b = book_store.read(id);
    if (b.stock < qty) { fail(); return; }
    b.stock -= qty;
    double total = b.price * qty;
    book_store.write(id, b);
    finance_append(total, 0.0);
    print_price(total);
    std::cout << '\n';
}

static void cmd_select(const std::vector<std::string> &args) {
    if (current_priv() < 3) { fail(); return; }
    if (args.size() != 2) { fail(); return; }
    if (!valid_isbn(args[1])) { fail(); return; }
    auto v = isbn_index.find(ISBNKey(args[1]));
    int32_t id;
    if (v.empty()) {
        BookRec b;
        memset(&b, 0, sizeof(BookRec));
        set_str_field(b.isbn, 24, args[1]);
        b.price = 0.0;
        b.stock = 0;
        id = book_store.append(b);
        isbn_index.insert(ISBNKey(args[1]), id);
    } else {
        id = v[0];
    }
    login_stack.back().selected_book_id = id;
}

static void cmd_modify(const std::vector<std::string> &args) {
    if (current_priv() < 3) { fail(); return; }
    if (args.size() < 2) { fail(); return; }
    if (login_stack.empty() || login_stack.back().selected_book_id == -1) { fail(); return; }

    bool has_isbn = false, has_name = false, has_author = false, has_kw = false, has_price = false;
    std::string n_isbn, n_name, n_author, n_kw_raw;
    std::vector<std::string> n_kws;
    double n_price = 0;

    for (size_t i = 1; i < args.size(); i++) {
        const std::string &t = args[i];
        if (t.rfind("-ISBN=", 0) == 0) {
            if (has_isbn) { fail(); return; }
            has_isbn = true;
            n_isbn = t.substr(6);
            if (!valid_isbn(n_isbn)) { fail(); return; }
        } else if (t.rfind("-name=", 0) == 0) {
            if (has_name) { fail(); return; }
            has_name = true;
            if (!extract_quoted(t, "-name=", n_name)) { fail(); return; }
            if (!valid_bookstr(n_name)) { fail(); return; }
        } else if (t.rfind("-author=", 0) == 0) {
            if (has_author) { fail(); return; }
            has_author = true;
            if (!extract_quoted(t, "-author=", n_author)) { fail(); return; }
            if (!valid_bookstr(n_author)) { fail(); return; }
        } else if (t.rfind("-keyword=", 0) == 0) {
            if (has_kw) { fail(); return; }
            has_kw = true;
            if (!extract_quoted(t, "-keyword=", n_kw_raw)) { fail(); return; }
            if (!parse_keyword_list(n_kw_raw, n_kws, true)) { fail(); return; }
            // Check for duplicates
            std::vector<std::string> sorted = n_kws;
            std::sort(sorted.begin(), sorted.end());
            for (size_t j = 1; j < sorted.size(); j++) if (sorted[j] == sorted[j - 1]) { fail(); return; }
        } else if (t.rfind("-price=", 0) == 0) {
            if (has_price) { fail(); return; }
            has_price = true;
            std::string v = t.substr(7);
            if (!parse_price(v, n_price)) { fail(); return; }
        } else {
            fail();
            return;
        }
    }
    if (!has_isbn && !has_name && !has_author && !has_kw && !has_price) { fail(); return; }

    int32_t id = login_stack.back().selected_book_id;
    BookRec b = book_store.read(id);

    if (has_isbn) {
        std::string cur = to_str_field(b.isbn, 24);
        if (cur == n_isbn) { fail(); return; }
        // check not exists
        auto e = isbn_index.find(ISBNKey(n_isbn));
        if (!e.empty()) { fail(); return; }
    }

    // All validations pass. Apply changes.
    remove_book_indexes(b, id);
    if (has_isbn) set_str_field(b.isbn, 24, n_isbn);
    if (has_name) set_str_field(b.name, 64, n_name);
    if (has_author) set_str_field(b.author, 64, n_author);
    if (has_kw) set_str_field(b.keyword, 64, n_kw_raw);
    if (has_price) b.price = n_price;
    add_book_indexes(b, id);
    book_store.write(id, b);
}

static void cmd_import(const std::vector<std::string> &args) {
    if (current_priv() < 3) { fail(); return; }
    if (args.size() != 3) { fail(); return; }
    if (login_stack.empty() || login_stack.back().selected_book_id == -1) { fail(); return; }
    int qty;
    if (!parse_uint32(args[1], qty) || qty <= 0) { fail(); return; }
    double cost;
    if (!parse_price(args[2], cost) || cost <= 0) { fail(); return; }
    int32_t id = login_stack.back().selected_book_id;
    BookRec b = book_store.read(id);
    b.stock += qty;
    book_store.write(id, b);
    finance_append(0.0, cost);
}

static void cmd_show_finance(const std::vector<std::string> &args) {
    if (current_priv() < 7) { fail(); return; }
    // args[0] = "show", args[1] = "finance", optional args[2] = count
    if (args.size() == 2) {
        double inc = 0, exp = 0;
        finance_get_sum(finance_count, inc, exp);
        std::cout << "+ ";
        print_price(inc);
        std::cout << " - ";
        print_price(exp);
        std::cout << '\n';
        return;
    }
    if (args.size() != 3) { fail(); return; }
    int n;
    if (!parse_uint32(args[2], n)) { fail(); return; }
    if (n > finance_count) { fail(); return; }
    if (n == 0) {
        std::cout << '\n';
        return;
    }
    double inc = 0, exp = 0;
    finance_get_sum(n, inc, exp);
    std::cout << "+ ";
    print_price(inc);
    std::cout << " - ";
    print_price(exp);
    std::cout << '\n';
}

// --------------------- Main loop ---------------------

static void process(const std::string &line) {
    auto args = smart_tokens(line);
    if (args.empty()) return;
    const std::string &cmd = args[0];
    if (cmd == "quit" || cmd == "exit") {
        if (args.size() != 1) { fail(); return; }
        close_storage();
        std::exit(0);
    } else if (cmd == "su") cmd_su(args);
    else if (cmd == "logout") cmd_logout(args);
    else if (cmd == "register") cmd_register(args);
    else if (cmd == "passwd") cmd_passwd(args);
    else if (cmd == "useradd") cmd_useradd(args);
    else if (cmd == "delete") cmd_delete(args);
    else if (cmd == "show") {
        if (args.size() >= 2 && args[1] == "finance") {
            cmd_show_finance(args);
        } else {
            cmd_show(args);
        }
    } else if (cmd == "buy") cmd_buy(args);
    else if (cmd == "select") cmd_select(args);
    else if (cmd == "modify") cmd_modify(args);
    else if (cmd == "import") cmd_import(args);
    else if (cmd == "report") {
        // report finance | report employee - privilege 7
        if (current_priv() < 7) { fail(); return; }
        if (args.size() == 2 && (args[1] == "finance" || args[1] == "employee")) {
            // Self-defined format: no specific output required, but let's stay quiet
        } else {
            fail();
        }
    } else if (cmd == "log") {
        if (current_priv() < 7) { fail(); return; }
        if (args.size() != 1) { fail(); return; }
    } else {
        fail();
    }
}

int main() {
    std::ios_base::sync_with_stdio(false);
    std::cin.tie(nullptr);
    init_storage();
    std::string line;
    while (std::getline(std::cin, line)) {
        // Trim trailing \r
        while (!line.empty() && (line.back() == '\r' || line.back() == '\n')) line.pop_back();
        process(line);
    }
    close_storage();
    return 0;
}
