#pragma once

#include <string>
#include <memory>
#include <vector>
#include <functional>
#include <variant>
#include <iostream>
#include <string_view>
#include <map>
#include <sstream>
#include <deque>
#include <cassert>
#include <utility>
#include <span>

template <typename I, typename F, typename S, typename U>
class WESL {
public:
	typedef uint32_t Size;

	typedef int Type;

	static inline const Type Any = 0;
	static inline const Type Bool = 1;
	static inline const Type Integer = 2;
	static inline const Type Float = 3;
	static inline const Type String = 4;
	static inline const Type UserData = 5;

	// Span types are value type codes scaled up
	static inline const Type SpanTypes = 100;

	static std::string typeName(Type t) {
		switch (t) {
			case Any: return "Any";
			case Bool: return "Bool";
			case Integer: return "Integer";
			case Float: return "Float";
			case String: return "String";
			case UserData: return "UserData";
		}
		return t > SpanTypes ? "Span[" + typeName(t/SpanTypes) + "]": "(bad type code)";
	}

	static Type toSpanType(Type t) {
		assert(t < SpanTypes);
		return t*SpanTypes;
	}

	static Type fromSpanType(Type t) {
		assert(t > SpanTypes);
		return t/SpanTypes;
	}

	// Runtime decayed function. See FunctionDefinition{}
	struct Function {
		Size entry = 0; // offset into byte code vector
		Size frame = 0; // size of stack frame: locals + array cells
		Size args = 0;  // number of leading argument locals
	};

	// Runtime decayed span. See compileSpan() calls
	struct Span {
		Size start = 0;
		Size width = 0;
		bool operator==(const Span& o) const { return start == o.start && width == o.width; }
		bool operator!=(const Span& o) const { return start != o.start || width != o.width; }
		bool operator<(const Span& o) const { return start < o.start; }
		bool operator<=(const Span& o) const { return start <= o.start; }
		bool operator>(const Span& o) const { return start > o.start; }
		bool operator>=(const Span& o) const { return start >= o.start; }
	};

	// Runtime stack holds values
	typedef std::variant<bool,I,F,S,U,Span> Value;

	friend std::ostream& operator<<(std::ostream& os, const Value& v) {
		struct Printer {
			std::ostream& os;
			Printer(std::ostream& o) : os{o} {}
			void operator() (const bool& v) { os << std::string(v ? "true": "false"); }
			void operator() (const I& v) { os << v; }
			void operator() (const F& v) { os << v; }
			void operator() (const S& v) { os << v; }
			void operator() (const U& v) { os << "(userdata)"; }
			void operator() (const Span& v) { os << "(span:" + std::to_string(v.start) + ":" + std::to_string(v.width) + ")"; }
		};
		std::visit(Printer(os), v);
		return os;
	}

	// External code calling an internal function
	struct CallIn {
		Size entry = 0;
		std::vector<Type> args;
		Size frame = 0;
	};

	class Session;

	// Internal code calling an external function
	struct CallOut {
		Type type;
		std::vector<Type> args;
		std::function<Value(Session&,std::span<Value>)> fn;
	};

	struct Library {
		std::map<std::string,CallOut> functions;

		void define(std::string_view name, Type type, std::vector<Type> args, std::function<Value(Session&,std::span<Value>)> fn) {
			functions[std::string(name)] = {type,args,fn};
		}
	};

	std::vector<Library*> libs;

	void link(Library& lib) {
		libs.push_back(&lib);
	}

private:

	// A persistent stack frame holding globals. Also see globalFunction
	Size globalFrame = 0;

	std::map<std::string,CallIn> callins;

	struct Token {
		enum {
			None,
			Symbol,
			StringLit,
			IntLit,
			FloatLit,
			Punct,
		} type = None;
		std::string_view str;

		explicit operator bool() const { return type != None; }

		friend std::ostream& operator<<(std::ostream& os, const Token& t) {
			os << [&]() {
				switch (t.type) {
					case Token::None: return "None";
					case Token::Symbol: return "Symbol";
					case Token::StringLit: return "StringLit";
					case Token::IntLit: return "IntLit";
					case Token::FloatLit: return "FloatLit";
					case Token::Punct: return "Punct";
				}
				throw;
			}() << ',' << t.str;
			return os;
		}
	};

	static std::string tokenDump(const Token& t) {
		switch (t.type) {
			case Token::None: return "None:" + std::string(t.str);
			case Token::Symbol: return "Symbol:" + std::string(t.str);
			case Token::StringLit: return "StringLit:" + std::string(t.str);
			case Token::IntLit: return "IntLit:" + std::string(t.str);
			case Token::FloatLit: return "FloatLit:" + std::string(t.str);
			case Token::Punct: return "Punct:" + std::string(t.str);
		}
		throw;
	}

	std::vector<Token> tokeniseSourceCode(std::string_view src) {
		using namespace std;

		vector<Token> tokens;

		auto peek = [&]() -> char {
			return src.size() ? src.front(): char(0);
		};

		auto peek2 = [&]() -> char {
			return src.size() > 1 ? src[1]: char(0);
		};

		auto take = [&]() -> char {
			if (src.size()) {
				char c = src.front();
				src.remove_prefix(1);
				return c;
			}
			return char(0);
		};

		auto iswhite = [&]() {
			return isspace(peek());
		};

		auto iscomment = [&]() {
			return (peek() == '/' && peek2() == '/');
		};

		auto skipwhite = [&]() {
			while (iswhite()) take();
		};

		auto skipcomment = [&]() {
			while (iscomment()) {
				take();
				take();
				while (peek() && peek() != '\n') take();
			}
		};

		auto skipstuff = [&]() {
			while (iswhite() || iscomment()) {
				skipwhite();
				skipcomment();
			}
		};

		auto token = [&]() -> Token {
			skipstuff();
			if (!peek()) return Token();
			const char* start = src.data();
			if (isalpha(peek())) {
				while (isalnum(peek()) || peek() == '_') take();
				return {Token::Symbol,string_view(start,src.data()-start)};
			}
			if (peek() == '"') {
				take();
				while (peek() && peek() != '"') {
					if (peek() == '\\') take();
					take();
				}
				if (peek() == '"') take();
				return {Token::StringLit,string_view(start,src.data()-start)};
			}
			if (isdigit(peek())) {
				bool flit = false;
				while (isdigit(peek()) || peek() == '.') {
					flit = flit || peek() == '.';
					take();
				}
				return {flit ? Token::FloatLit: Token::IntLit, string_view(start,src.data()-start)};
			}
			take();
			return {Token::Punct,string_view(start,1)};
		};

		while (src.size()) tokens.push_back(token());
		if (tokens.size() && !tokens.back()) tokens.pop_back();

		return tokens;
	}

	static bool isType(const Token& t) {
		return t.type == Token::Symbol && (
			t.str == "void" ||
			t.str == "int" ||
			t.str == "bool" ||
			t.str == "float" ||
			t.str == "string" ||
			t.str == "udata" ||
			t.str == "ints" ||
			t.str == "bools" ||
			t.str == "floats" ||
			t.str == "strings" ||
			t.str == "udatas"
		);
	}

	static Type tokenType(const Token& t) {
		if (t.str == "void") return Any;
		if (t.str == "int") return Integer;
		if (t.str == "bool") return Bool;
		if (t.str == "float") return Float;
		if (t.str == "string") return String;
		if (t.str == "udata") return UserData;
		if (t.str == "ints") return toSpanType(Integer);
		if (t.str == "bools") return toSpanType(Bool);
		if (t.str == "floats") return toSpanType(Float);
		if (t.str == "strings") return toSpanType(String);
		if (t.str == "udatas") return toSpanType(UserData);
		throw;
	}

	static bool isName(const Token& t) {
		return t.type == Token::Symbol && !isType(t);
	}

	static bool isPunct(const Token& t, char c) {
		return t.type == Token::Punct && c == t.str.front();
	}

	static bool isOpeningParen(const Token& t) {
		return isPunct(t, '(');
	}

	static bool isClosingParen(const Token& t) {
		return isPunct(t, ')');
	}

	static bool isOpeningBrace(const Token& t) {
		return isPunct(t, '{');
	}

	static bool isClosingBrace(const Token& t) {
		return isPunct(t, '}');
	}

	static bool isOpeningSquareBracket(const Token& t) {
		return isPunct(t, '[');
	}

	static bool isClosingSquareBracket(const Token& t) {
		return isPunct(t, ']');
	}

	static bool isComma(const Token& t) {
		return isPunct(t, ',');
	}

	static bool isAssignment(const Token& t) {
		return isPunct(t, '=');
	}

	static bool isColon(const Token& t) {
		return isPunct(t, ':');
	}

	static bool isSemiColon(const Token& t) {
		return isPunct(t, ';');
	}

	enum class Opcode {
		Print,
		AddI,
		AddF,
		AddS,
		SubI,
		SubF,
		MulI,
		MulF,
		DivI,
		DivF,
		ModI,
		Lt,
		Gt,
		Lte,
		Gte,
		Eq,
		NEq,
		Literal,
		Drop,
		Jump,
		JZ,
		JNZ,
		LStore,
		LFetch,
		GStore,
		GFetch,
		Call,
		Return,
		CallOut,
		LSpan,
		GSpan,
		Get,
		Set,
		ICast,
		FCast,
		BCast,
		SizeOf,
		Times,
		Range,
	};

	// "byte code"
	std::vector<std::variant<Opcode,Size,bool,I,S,F,Function,CallOut*>> code;

	struct VariableDefinition {
		std::string name;
		Type type;
		Size offset = 0; // into a stack frame
		Size width = 0; // used by arrays
		bool spanIndirect = false;
	};

	struct BlockDefinition {
		std::map<std::string,VariableDefinition> variables;
		std::vector<Size> cjumps; // resolve continue branches
		std::vector<Size> bjumps; // resolve break branches
	};

	struct FunctionDefinition {
		std::string name;
		Type type;
		Size entry = 0; // offset into byte code vector
		Size frame = 0; // number of locals + array cells
		std::vector<Type> args;
		std::deque<BlockDefinition> blocks;
	};

	void processSourceCode(std::string_view src) {
		using namespace std;

		auto tokens = tokeniseSourceCode(src);

		span<const Token> cursor = tokens;
		if (!cursor.size()) return;

		// Statements and expression boundaries update 'here'
		auto here = cursor.front();

		// Error message prefix for a token. Usually where(here)
		auto where = [&](const Token& t) {
			int line = 1;
			const char* p = src.data();
			while (p < t.str.data()) {
				if (*p++ == '\n') line++;
			}
			return "line " + std::to_string(line);
		};

		// There is no reflection; most compile-time info is discarded
		// FunctionDefinition decays to Function{}
		// SpanTypes decay to Span{}
		std::map<std::string,FunctionDefinition> functions;
		std::map<std::string,Size> enums;

		// Global "scope" is a top-level nameless function+block with
		// a stack frame that persists across ::execute() invocations
		auto globalFunction = &functions[""];
		FunctionDefinition* currentFunc = globalFunction;

		// Block scope
		vector<BlockDefinition*> blocks;

		// recursive descent
		std::function<Type(Type)> takeExpression;
		std::function<void(void)> takeStatement;

		auto beginBlock = [&]() {
			assert(currentFunc);
			currentFunc->blocks.emplace_back();
			blocks.push_back(&currentFunc->blocks.back());
		};

		auto endBlock = [&](Size cjump, Size bjump) {
			assert(blocks.size() > 1);
			for (auto mark: currentFunc->blocks.back().cjumps) code[mark] = cjump;
			for (auto mark: currentFunc->blocks.back().bjumps) code[mark] = bjump;
			currentFunc->blocks.pop_back();
			blocks.pop_back();
		};

		auto currentBlock = [&]() -> BlockDefinition& {
			assert(blocks.size());
			return *blocks.back();
		};

		auto peekName = [&]() {
			return cursor.size() > 0 && isName(cursor[0]);
		};

		auto peekFunctionDefinition = [&]() {
			return cursor.size() >= 3
				&& isType(cursor[0])
				&& isName(cursor[1])
				&& isOpeningParen(cursor[2])
			;
		};

		auto peekVariableDefinition = [&]() {
			return cursor.size() >= 3
				&& isType(cursor[0])
				&& isName(cursor[1])
				&& (isAssignment(cursor[2]) || isSemiColon(cursor[2]))
			;
		};

		auto peekArrayDefinition = [&]() {
			return cursor.size() >= 3
				&& isType(cursor[0])
				&& isName(cursor[1])
				&& isPunct(cursor[2],'[');
			;
		};

		auto peekVariableMutation = [&]() {
			return cursor.size() >= 2
				&& isName(cursor[0])
				&& isAssignment(cursor[1])
			;
		};

		auto peekVariableMutationAssign = [&]() {
			return cursor.size() >= 3
				&& isName(cursor[0])
				&& (isPunct(cursor[1],'+') || isPunct(cursor[1],'-') || isPunct(cursor[1],'*') || isPunct(cursor[1],'/'))
				&& isAssignment(cursor[2])
			;
		};

		auto peekSpanMutation = [&]() {
			return cursor.size() >= 2
				&& isName(cursor[0])
				&& isPunct(cursor[1],'[')
			;
		};

		auto peekFunctionCall = [&]() {
			return cursor.size() >= 2
				&& isName(cursor[0])
				&& isPunct(cursor[1],'(')
			;
		};

		auto peekWord = [&](const string& word) {
			return cursor.size() > 0 && cursor[0].str == word;
		};

		auto peekReturn = [&]() {
			return peekWord("return");
		};

		auto peekPrint = [&]() {
			return peekWord("print");
		};

		auto peekEnumDefinition = [&]() {
			return peekWord("enum");
		};

		auto peekBool = [&]() {
			return peekWord("true") || peekWord("false");
		};

		auto peekIf = [&]() {
			return peekWord("if");
		};

		auto peekElse = [&]() {
			return peekWord("else");
		};

		auto peekFor = [&]() {
			return peekWord("for");
		};

		auto peekForRange = [&]() {
			return cursor.size() > 5
				&& peekFor()
				&& isOpeningParen(cursor[1])
				&& isType(cursor[2])
				&& isName(cursor[3])
				&& isColon(cursor[4])
			;
		};

		auto peekWhile = [&]() {
			return peekWord("while");
		};

		auto peekContinue = [&]() {
			return peekWord("continue");
		};

		auto peekBreak = [&]() {
			return peekWord("break");
		};

		auto peekSizeOf = [&]() {
			return peekWord("sizeof");
		};

		auto peekEnumItem = [&]() {
			return cursor.size() > 0 && cursor[0].type == Token::Symbol && enums.find(string(cursor[0].str)) != enums.end();
		};

		auto peekIntegerLiteral = [&]() {
			return cursor.size() > 0 && cursor[0].type == Token::IntLit;
		};

		auto peekFloatLiteral = [&]() {
			return cursor.size() > 0 && cursor[0].type == Token::FloatLit;
		};

		auto peekStringLiteral = [&]() {
			return cursor.size() > 0 && cursor[0].type == Token::StringLit;
		};

		auto peekCast = [&]() {
			return cursor.size() > 1 && isType(cursor[0]) && isPunct(cursor[1],'(');
		};

		auto peekAnd = [&]() {
			return peekWord("and");
		};

		auto peekOr = [&]() {
			return peekWord("or");
		};

		auto peek = [&]() -> Token {
			return cursor.size() ? cursor.front(): Token();
		};

		auto take = [&]() -> Token {
			if (cursor.size()) {
				Token t = cursor.front();
				cursor = cursor.subspan(1);
				return t;
			}
			return Token();
		};

		auto takeType = [&]() {
			if (!isType(peek()))
				throw runtime_error(where(here) + ": expected type: " + tokenDump(peek()));
			return tokenType(take());
		};

		auto takeOpeningParen = [&]() {
			if (!isOpeningParen(peek()))
				throw runtime_error(where(here) + ": expected opening paren: " + tokenDump(peek()));
			take();
		};

		auto takeClosingParen = [&]() {
			if (!isClosingParen(peek()))
				throw runtime_error(where(here) + ": expected closing paren: " + tokenDump(peek()));
			take();
		};

		auto takeOpeningBrace = [&]() {
			if (!isOpeningBrace(peek()))
				throw runtime_error(where(here) + ": expected opening brace: " + tokenDump(peek()));
			take();
		};

		auto takeClosingBrace = [&]() {
			if (!isClosingBrace(peek()))
				throw runtime_error(where(here) + ": expected closing brace: " + tokenDump(peek()));
			take();
		};

		auto takeOpeningSquareBracket = [&]() {
			if (!isOpeningSquareBracket(peek()))
				throw runtime_error(where(here) + ": expected opening square bracket: " + tokenDump(peek()));
			take();
		};

		auto takeClosingSquareBracket = [&]() {
			if (!isClosingSquareBracket(peek()))
				throw runtime_error(where(here) + ": expected closing square bracket: " + tokenDump(peek()));
			take();
		};

		auto takeAssignment = [&]() {
			if (!isPunct(peek(),'='))
				throw runtime_error(where(here) + ": expected assignment operator: " + tokenDump(peek()));
			take();
		};

		auto takeSemiColon = [&]() {
			if (!isPunct(peek(),';'))
				throw runtime_error(where(here) + ": expected semi-colon: " + tokenDump(peek()));
			take();
		};

		auto takeStaticOffset = [&]() -> Size {
			if (peekIntegerLiteral())
				return Size(max(0,stoi(string(take().str))));

			if (peekEnumItem())
				return enums[string(take().str)];

			throw runtime_error(where(here) + ": expected static offset: " + tokenDump(peek()));
		};

		auto tryTakeComma = [&]() {
			if (isComma(peek())) take();
		};

		auto tryTakeSemiColon = [&]() {
			if (isSemiColon(peek())) take();
		};

		auto compileSpan = [&](auto& variable, bool global) {
			code.emplace_back(global ? Opcode::GSpan: Opcode::LSpan);
			code.emplace_back(variable.offset);
			code.emplace_back(variable.width);
		};

		struct FoundVariable {
			VariableDefinition* variable = nullptr;
			bool global = false;
		};

		auto findVariable = [&](const string& name) -> FoundVariable {
			assert(blocks.size());
			for (int i = int(blocks.size())-1; i >= 0; --i) {
				if (blocks[i]->variables.find(name) != blocks[i]->variables.end())
					return {&blocks[i]->variables[name], i == 0};
			}
			return {};
		};

		auto takeBlock = [&]() {
			if (isOpeningBrace(peek())) {
				takeOpeningBrace();
				while (!isClosingBrace(peek())) {
					takeStatement();
				}
				takeClosingBrace();
			}
			else {
				takeStatement();
			}
		};

		takeExpression = [&](Type rtype) {
			here = cursor.front();

			// takeExpression(Any) lazily converts to the first type that is found.
			// Eg, 1+1.0 will fail because the leading integer enforces the type for
			// the remainder of the expression. All casts must be explicit
			auto typeCheck = [&](Type type) {
				if (rtype == Any) rtype = type;

				if (rtype != type)
					throw runtime_error(where(here) + " found " + typeName(type) + "; expected " + typeName(rtype));
			};

			enum class Operator {
				Add,
				Sub,
				Mul,
				Div,
				Mod,
				Lt,
				Gt,
				Lte,
				Gte,
				Eq,
				NEq,
			};

			// shunting yard
			vector<Operator> operators;

			auto operatorSymbol = [](Operator op) {
				switch (op) {
					case Operator::Add: return "+";
					case Operator::Sub: return "-";
					case Operator::Mul: return "*";
					case Operator::Div: return "/";
					case Operator::Mod: return "%";
					case Operator::Lt: return "<";
					case Operator::Gt: return ">";
					case Operator::Lte: return "<=";
					case Operator::Gte: return ">=";
					case Operator::Eq: return "==";
					case Operator::NEq: return "!=";
				}
			};

			auto operatorPrecedence = [](Operator op) -> int {
				switch (op) {
					case Operator::Add: return 2;
					case Operator::Sub: return 2;
					case Operator::Mul: return 3;
					case Operator::Div: return 3;
					case Operator::Mod: return 3;
					case Operator::Lt: return 1;
					case Operator::Gt: return 1;
					case Operator::Lte: return 1;
					case Operator::Gte: return 1;
					case Operator::Eq: return 1;
					case Operator::NEq: return 1;
				}
				throw;
			};

			auto opcodeInteger = [&](Operator op, Opcode i) {
				if (rtype == Integer) return i;
				throw runtime_error(where(here) + ": no opcode for operator " + operatorSymbol(op) + " on type " + typeName(rtype));
			};

			auto opcodeIntegerFloat = [&](Operator op, Opcode i, Opcode f) {
				switch (rtype) {
					case Integer: return i;
					case Float: return f;
					default: break;
				}
				throw runtime_error(where(here) + ": no opcode for operator " + operatorSymbol(op) + " on type " + typeName(rtype));
			};

			auto opcodeIntegerFloatString = [&](Operator op, Opcode i, Opcode f, Opcode s) {
				switch (rtype) {
					case Integer: return i;
					case Float: return f;
					case String: return s;
					default: break;
				}
				throw runtime_error(where(here) + ": no opcode for operator " + operatorSymbol(op) + " on type " + typeName(rtype));
			};

			auto operatorToOpcode = [&](Operator op) -> Opcode {
				switch (op) {
					case Operator::Add: return opcodeIntegerFloatString(op, Opcode::AddI, Opcode::AddF, Opcode::AddS);
					case Operator::Sub: return opcodeIntegerFloat(op, Opcode::SubI, Opcode::SubF);
					case Operator::Mul: return opcodeIntegerFloat(op, Opcode::MulI, Opcode::MulF);
					case Operator::Div: return opcodeIntegerFloat(op, Opcode::DivI, Opcode::DivF);
					case Operator::Mod: return opcodeInteger(op, Opcode::ModI);
					case Operator::Lt: return Opcode::Lt;
					case Operator::Gt: return Opcode::Gt;
					case Operator::Lte: return Opcode::Lte;
					case Operator::Gte: return Opcode::Gte;
					case Operator::Eq: return Opcode::Eq;
					case Operator::NEq: return Opcode::NEq;
				}
				throw;
			};

			// shunting yard. Compile any previous operators per precedence, and cache the new one
			auto pushOperator = [&](Operator op) {
				while (operators.size() && operatorPrecedence(operators.back()) >= operatorPrecedence(op)) {
					code.emplace_back(operatorToOpcode(operators.back()));
					operators.pop_back();
				}
				operators.push_back(op);
			};

			auto peekOperator = [&]() -> string {
				// The tokeniser has already split two-char operators into separate tokens
				if (cursor.size() > 1 && cursor[0].type == Token::Punct && cursor[1].type == Token::Punct) {
					char a = cursor[0].str.front();
					char b = cursor[1].str.front();
					if (a == '=' && b == '=') { return "=="; }
					if (a == '<' && b == '=') { return "<="; }
					if (a == '>' && b == '=') { return ">="; }
					if (a == '!' && b == '=') { return "!="; }
				}
				if (cursor.size() > 0 && cursor[0].type == Token::Punct) {
					char a = cursor[0].str.front();
					if (a == '+') { return "+"; }
					if (a == '-') { return "-"; }
					if (a == '*') { return "*"; }
					if (a == '/') { return "/"; }
					if (a == '%') { return "%"; }
					if (a == '<') { return "<"; }
					if (a == '>') { return ">"; }
				}
				return "";
			};

			auto compileFunctionCall = [&](FunctionDefinition& func) {
				takeOpeningParen();

				for (auto& atype: func.args) {
					if (isClosingParen(peek()))
						throw runtime_error(where(here) + ": expected " + std::to_string(func.args.size()) + " arguments");
					takeExpression(atype);
				}

				takeClosingParen();
				typeCheck(func.type);

				code.emplace_back(Opcode::Call);
				// Run-time has no return/argument type info retained, just the mechanics
				code.emplace_back(Function{func.entry,func.frame,Size(func.args.size())});
			};

			auto compileVariableReference = [&](auto& variable, bool global) {
				if (variable.type > SpanTypes && !variable.spanIndirect) {
					code.emplace_back(global ? Opcode::GSpan: Opcode::LSpan);
					code.emplace_back(variable.offset);
					code.emplace_back(variable.width);
				}
				else {
					code.emplace_back(global ? Opcode::GFetch: Opcode::LFetch);
					code.emplace_back(variable.offset);
				}

				if (isOpeningSquareBracket(peek())) {
					// It's a span cell access
					take();
					typeCheck(fromSpanType(variable.type));
					takeExpression(Integer);
					code.emplace_back(Opcode::Get);
					takeClosingSquareBracket();
				}
				else {
					// It's a regular scalar
					typeCheck(variable.type);
				}
			};

			// shunting yard loop
			do {
				here = cursor.front();
				string op = peekOperator();

				if (rtype != Any && op.size()) {
					for (size_t i = 0; i < op.size(); i++) take();

					if (op == "+") { pushOperator(Operator::Add); }
					else if (op == "-") { pushOperator(Operator::Sub); }
					else if (op == "*") { pushOperator(Operator::Mul); }
					else if (op == "/") { pushOperator(Operator::Div); }
					else if (op == "%") { pushOperator(Operator::Mod); }
					else if (op == "<") { pushOperator(Operator::Lt); }
					else if (op == ">") { pushOperator(Operator::Gt); }
					else if (op == "<=") { pushOperator(Operator::Lte); }
					else if (op == ">=") { pushOperator(Operator::Gte); }
					else if (op == "==") { pushOperator(Operator::Eq); }
					else if (op == "!=") { pushOperator(Operator::NEq); }
					else throw runtime_error(where(here) + ": unexpected operator " + op + " for type " + typeName(rtype));
				}

				// Nested parens. The inner expression must match our type
				if (isOpeningParen(peek())) {
					take();
					typeCheck(takeExpression(rtype));
					takeClosingParen();
					continue;
				}

				if (peekEnumItem()) {
					typeCheck(Integer);
					code.emplace_back(Opcode::Literal);
					code.emplace_back(I(enums[string(take().str)]));
					continue;
				}

				if (peekBool()) {
					typeCheck(Bool);
					code.emplace_back(Opcode::Literal);
					code.emplace_back(take().str == "true");
					continue;
				}

				if (peekIntegerLiteral()) {
					typeCheck(Integer);
					code.emplace_back(Opcode::Literal);
					code.emplace_back(I(stoi(string(take().str))));
					continue;
				}

				if (peekFloatLiteral()) {
					typeCheck(Float);
					code.emplace_back(Opcode::Literal);
					code.emplace_back(F(stof(string(take().str))));
					continue;
				}

				if (peekStringLiteral()) {
					typeCheck(String);
					string lit;
					string_view str = take().str;
					str.remove_prefix(1);
					str.remove_suffix(1);
					for (auto it = str.begin(); it != str.end(); ) {
						char c = *it++;
						if (c == '\\') {
							c = *it++;
							if (c == 'r') c = '\r';
							if (c == 'n') c = '\n';
							if (c == 't') c = '\t';
						}
						lit += c;
					}
					code.emplace_back(Opcode::Literal);
					code.emplace_back(S(lit));
					continue;
				}

				if (peekSizeOf()) {
					typeCheck(Integer);
					take();

					bool parens = isOpeningParen(peek());
					if (parens) takeOpeningParen();
					auto type = takeExpression(Any);
					if (parens) takeClosingParen();

					if (type <= SpanTypes)
						throw runtime_error(where(here) + ": sizeof() only works on arrays");

					code.emplace_back(Opcode::SizeOf);
					continue;
				}

				if (peekCast()) {
					Type type = takeType();
					typeCheck(type);

					takeOpeningParen();
					auto cast = takeExpression(Any);
					takeClosingParen();

					if (type != cast) {
						if (type == Integer && cast == Float) {
							code.emplace_back(Opcode::ICast);
							continue;
						}
						if (type == Float && cast == Integer) {
							code.emplace_back(Opcode::FCast);
							continue;
						}
						if (type == Bool) {
							code.emplace_back(Opcode::BCast);
							continue;
						}
						throw runtime_error(where(here) + ": cannot cast from " + typeName(cast) + " to " + typeName(type));
					}
					continue;
				}

				if (peekName()) {
					string name(take().str);
					auto found = findVariable(name);

					if (found.variable) {
						compileVariableReference(*found.variable, found.global);
						continue;
					}

					if (functions.find(name) != functions.end()) {
						auto& func = functions[name];
						compileFunctionCall(func);
						continue;
					}

					if ([&]() {
						for (auto lib: libs) {
							if (lib->functions.find(name) != lib->functions.end()) {
								auto& func = lib->functions[name];

								takeOpeningParen();

								for (auto& atype: func.args) {
									if (isClosingParen(peek()))
										throw runtime_error(where(here) + ": expected " + std::to_string(func.args.size()) + " arguments");
									takeExpression(atype);
								}

								takeClosingParen();
								typeCheck(func.type);
								code.emplace_back(Opcode::CallOut);
								code.emplace_back(&func);
								return true;
							}
						}
						return false;
					}()) continue;

					throw runtime_error(where(here) + ": unknown symbol " + name);
					return Any;
				}

				throw runtime_error(where(here) + ": unknown token " + tokenDump(peek()));
				return Any;
			}
			while (peekOperator().size());

			// resolve remaining cached operators
			while (operators.size()) {
				code.emplace_back(operatorToOpcode(operators.back()));
				operators.pop_back();
			}

			tryTakeComma();
			return rtype;
		};

		auto takeVariableDefinition = [&]() {
			Type type = takeType();
			string name(take().str);

			if (currentBlock().variables.find(name) != currentBlock().variables.end())
				throw runtime_error(where(here) + ": duplicate name in scope: " + name);

			auto& variable = currentBlock().variables[name];
			variable.name = name;
			variable.type = type;
			variable.offset = currentFunc->frame++;

			if (type > SpanTypes)
				variable.spanIndirect = true;

			if (isAssignment(peek())) {
				take();
				takeExpression(variable.type);
				code.emplace_back(Opcode::LStore);
				code.emplace_back(variable.offset);
			}

			takeSemiColon();
		};

		auto takeVariableMutation = [&]() {
			string name(take().str);
			takeAssignment();

			auto found = findVariable(name);

			if (found.variable) {
				auto& variable = *found.variable;
				takeExpression(variable.type);
				code.emplace_back(found.global ? Opcode::GStore: Opcode::LStore);
				code.emplace_back(variable.offset);
				tryTakeSemiColon();
				return;
			}

			throw runtime_error(where(here) + ": unknown variable: " + name);
		};

		// += -= *= /=
		auto takeVariableMutationAssign = [&]() {
			string name(take().str);
			string op(take().str); // +*-/
			takeAssignment();

			auto found = findVariable(name);

			if (found.variable) {
				auto& variable = *found.variable;
				code.emplace_back(found.global ? Opcode::GFetch: Opcode::LFetch);
				code.emplace_back(variable.offset);
				takeExpression(variable.type);

				auto error = [&]() {
					throw runtime_error(where(here) + ": unsupported " + op + "= type " + typeName(variable.type));
				};

				switch (variable.type) {
					case Integer: {
						switch (op.front()) {
							case '+': code.emplace_back(Opcode::AddI); break;
							case '-': code.emplace_back(Opcode::SubI); break;
							case '*': code.emplace_back(Opcode::MulI); break;
							case '/': code.emplace_back(Opcode::DivI); break;
						}
						break;
					}
					case Float: {
						switch (op.front()) {
							case '+': code.emplace_back(Opcode::AddF); break;
							case '-': code.emplace_back(Opcode::SubF); break;
							case '*': code.emplace_back(Opcode::MulF); break;
							case '/': code.emplace_back(Opcode::DivF); break;
						}
						break;
					}
					case String: {
						switch (op.front()) {
							case '+': code.emplace_back(Opcode::AddF); break;
							default: error();
						}
						break;
					}
					default: error();
				}
				code.emplace_back(found.global ? Opcode::GStore: Opcode::LStore);
				code.emplace_back(variable.offset);
				tryTakeSemiColon();
				return;
			}

			throw runtime_error(where(here) + ": unknown variable: " + name);
		};

		auto takeArrayDefinition = [&]() {
			Type type = takeType();
			string name(take().str);

			if (currentBlock().variables.find(name) != currentBlock().variables.end())
				throw runtime_error(where(here) + ": duplicate name in scope: " + name);

			auto& variable = currentBlock().variables[name];
			variable.name = name;
			variable.type = toSpanType(type);
			variable.offset = currentFunc->frame;

			takeOpeningSquareBracket();
			variable.width = takeStaticOffset();
			currentFunc->frame += variable.width;
			takeClosingSquareBracket();

			if (isAssignment(peek())) {
				take();
				takeOpeningSquareBracket();
				while (!isClosingSquareBracket(peek())) {
					here = cursor.front();

					compileSpan(variable, blocks.size() == 1);

					Size index = takeStaticOffset();

					if (index >= variable.width)
						throw runtime_error(where(here) + ": array index out of bounds: " + tokenDump(peek()));

					code.emplace_back(Opcode::Literal);
					code.emplace_back(I(index));

					takeAssignment();

					takeExpression(fromSpanType(variable.type));
					code.emplace_back(Opcode::Set);
					tryTakeComma();
				}
				takeClosingSquareBracket();
			}

			takeSemiColon();
		};

		auto takeSpanMutation = [&]() {
			string name(take().str);
			auto found = findVariable(name);

			if (!found.variable)
				throw runtime_error(where(here) + ": unknown variable: " + name);

			auto& variable = *found.variable;
			compileSpan(variable, found.global);

			takeOpeningSquareBracket();
			takeExpression(Integer); // index
			takeClosingSquareBracket();

			takeAssignment();

			takeExpression(fromSpanType(variable.type));
			code.emplace_back(Opcode::Set);
			tryTakeSemiColon();
		};

		auto takePrint = [&]() {
			take();
			takeOpeningParen();
			int i = 0;
			while (peek() && !isClosingParen(peek()) && !isSemiColon(peek())) {
				if (i++) {
					code.emplace_back(Opcode::Literal);
					code.emplace_back(S(" "));
					code.emplace_back(Opcode::Print);
				}
				takeExpression(Any);
				code.emplace_back(Opcode::Print);
			}
			takeClosingParen();
			takeSemiColon();
			code.emplace_back(Opcode::Literal);
			code.emplace_back(S("\n"));
			code.emplace_back(Opcode::Print);
		};

		auto takeReturn = [&]() {
			take();

			if (currentFunc->type == Any && !isSemiColon(peek()))
				throw runtime_error(where(here) + ": returning value in void function");

			// everything returns something, even void functions, so insert an int(0)
			if (isSemiColon(peek())) {
				code.emplace_back(Opcode::Literal);
				code.emplace_back(I(0));
			}
			else {
				takeExpression(currentFunc->type);
			}

			code.emplace_back(Opcode::Return);
			takeSemiColon();
		};

		auto takeEnumDefinition = [&]() {
			take();
			takeOpeningBrace();
			while (!isClosingBrace(peek())) {
				if (!peekName())
					throw runtime_error(where(here) + ": expected name: " + tokenDump(peek()));
				string name(take().str);
				if (enums.find(name) != enums.end())
					throw runtime_error(where(here) + ": duplicate enum item: " + name);
				enums[name] = Size(enums.size());
				tryTakeComma();
			}
			takeClosingBrace();
			takeSemiColon();
		};

		auto takeCondition = [&]() {
			vector<Size> jumpPass;
			vector<Size> jumpFail;

			takeExpression(Any);

			auto branch = [&](Opcode op, auto& vec) {
				code.emplace_back(op);
				vec.push_back(code.size());
				code.emplace_back(Size(0));
			};

			if (peekAnd()) {
				while (peekAnd()) {
					take();
					branch(Opcode::JZ, jumpFail);
					takeExpression(Any);
				}
				branch(Opcode::JZ, jumpFail);

				if (peekOr())
					throw runtime_error(where(here) + ": mixed and/or chain");
			}
			else
			if (peekOr()) {
				while (peekOr()) {
					take();
					branch(Opcode::JNZ, jumpPass);
					takeExpression(Any);
				}
				branch(Opcode::JZ, jumpFail);

				if (peekAnd())
					throw runtime_error(where(here) + ": mixed and/or chain");
			}
			else {
				branch(Opcode::JZ, jumpFail);
			}

			return array<vector<Size>,2>{jumpPass, jumpFail};
		};

		auto takeIf = [&]() {
			take();
			takeOpeningParen();

			auto [jumpPass,jumpFail] = takeCondition();

			takeClosingParen();

			for (auto jump: jumpPass) {
				code[jump] = Size(code.size());
			}

			takeBlock();

			if (peekElse()) {
				take();
				code.emplace_back(Opcode::Jump);
				Size elseJump = code.size();
				code.emplace_back(Size(0));
				for (auto jump: jumpFail) {
					code[jump] = Size(code.size());
				}
				if (peekIf()) takeStatement(); else takeBlock();
				code[elseJump] = Size(code.size());
			}
			else {
				for (auto jump: jumpFail) {
					code[jump] = Size(code.size());
				}
			}
		};

		auto takeForRange = [&]() {
			take();
			beginBlock();

			takeOpeningParen();

			Type type = takeType();
			string name(take().str);

			if (currentBlock().variables.find(name) != currentBlock().variables.end())
				throw runtime_error(where(here) + ": duplicate name in scope: " + name);

			auto& variable = currentBlock().variables[name];
			variable.name = name;
			variable.type = type;
			variable.offset = currentFunc->frame++;

			if (variable.type > SpanTypes)
				throw runtime_error(where(here) + ": expected range of scalars");

			take(); // colon

			Type rtype = takeExpression(Any); // range on stack

			takeClosingParen();

			if (type == Integer && rtype == Integer) {
				// range is limit N
				code.emplace_back(Opcode::Literal);
				code.emplace_back(I(-1)); // index, 0:N-1
				Size begin = code.size();

				code.emplace_back(Opcode::Times);
				code.emplace_back(variable.offset);
				Size done = code.size();
				code.emplace_back();

				takeBlock();

				code.emplace_back(Opcode::Jump);
				code.emplace_back(Size(begin));
				code[done] = Size(code.size());

				endBlock(begin, code.size());
				code.emplace_back(Opcode::Drop); // index
				code.emplace_back(Opcode::Drop); // limit
				return;
			}

			if (rtype > SpanTypes && fromSpanType(rtype) == type) {
				Size begin = code.size();

				code.emplace_back(Opcode::Range);
				code.emplace_back(variable.offset);
				Size done = code.size();
				code.emplace_back();

				takeBlock();

				code.emplace_back(Opcode::Jump);
				code.emplace_back(Size(begin));
				code[done] = Size(code.size());

				endBlock(begin, code.size());
				code.emplace_back(Opcode::Drop); // range
				return;
			}

			throw runtime_error(where(here) + ": expected range of type " + typeName(type));
		};

		auto takeWhile = [&]() {
			take();
			beginBlock();

			Size begin = code.size();
			takeOpeningParen();

			auto [jumpPass,jumpFail] = takeCondition();

			takeClosingParen();

			for (auto jump: jumpPass) {
				code[jump] = Size(code.size());
			}

			takeBlock();

			code.emplace_back(Opcode::Jump);
			code.emplace_back(begin);

			for (auto jump: jumpFail) {
				code[jump] = Size(code.size());
			}

			endBlock(begin, code.size());
		};

		auto takeContinue = [&]() {
			take();
			code.emplace_back(Opcode::Jump);
			currentBlock().cjumps.push_back(code.size());
			code.emplace_back();
			takeSemiColon();
		};

		auto takeBreak = [&]() {
			take();
			code.emplace_back(Opcode::Jump);
			currentBlock().bjumps.push_back(code.size());
			code.emplace_back();
			takeSemiColon();
		};

		auto takeFunctionCall = [&]() {
			takeExpression(Any);
			code.emplace_back(Opcode::Drop);
			takeSemiColon();
		};

		takeStatement = [&]() {
			auto here = cursor.front();

			if (peekVariableDefinition()) {
				takeVariableDefinition();
				return;
			}

			if (peekVariableMutation()) {
				takeVariableMutation();
				return;
			}

			if (peekVariableMutationAssign()) {
				takeVariableMutationAssign();
				return;
			}

			if (peekArrayDefinition()) {
				takeArrayDefinition();
				return;
			}

			if (peekSpanMutation()) {
				takeSpanMutation();
				return;
			}

			if (peekEnumDefinition()) {
				takeEnumDefinition();
				return;
			}

			if (peekPrint()) {
				takePrint();
				return;
			}

			if (peekReturn()) {
				takeReturn();
				return;
			}

			if (peekIf()) {
				takeIf();
				return;
			}

			if (peekForRange()) {
				takeForRange();
				return;
			}

			if (peekWhile()) {
				takeWhile();
				return;
			}

			if (peekContinue()) {
				takeContinue();
				return;
			}

			if (peekBreak()) {
				takeBreak();
				return;
			}

			if (peekFunctionCall()) {
				takeFunctionCall();
				return;
			}

			throw runtime_error(where(here) + ": unexpected takeStatement: " + tokenDump(peek()));
		};

		auto takeFunctionDefinition = [&]() {
			// The global function is executed once to initialize global state.
			// Include a jump around each normal function's code segment
			code.emplace_back(Opcode::Jump);
			Size skip = Size(code.size());
			code.emplace_back();

			Type rtype = takeType();
			string name(take().str);

			if (functions.find(name) != functions.end())
				throw runtime_error(where(here) + ": duplicate function name: " + name);

			currentFunc = &functions[name];
			currentFunc->name = name;
			currentFunc->type = rtype;
			currentFunc->entry = code.size();
			currentFunc->args.clear();
			currentFunc->frame = 0;

			beginBlock();

			// Argument list
			takeOpeningParen();
			while (peek() && isType(peek())) {
				currentFunc->args.push_back(takeType());
				if (peekName()) {
					string name(take().str);
					auto& variable = currentBlock().variables[name];
					variable.name = name;
					variable.type = currentFunc->args.back();
					variable.offset = currentFunc->frame++;
					tryTakeComma();
					continue;
				}
				throw runtime_error(where(here) + ": expected argument name: " + tokenDump(peek()));
			}
			takeClosingParen();

			takeBlock();

			bool haveReturn = holds_alternative<Opcode>(code.back()) && get<Opcode>(code.back()) == Opcode::Return;

			// void functions that have no explicit return statement still return something
			// so the call/return stack handling balances out
			if (!haveReturn) {
				code.emplace_back(Opcode::Literal);
				code.emplace_back(I(0));
				code.emplace_back(Opcode::Return);
			}

			endBlock(currentFunc->entry, code.size());

			code[skip] = Size(code.size());
			currentFunc = globalFunction;
		};

		beginBlock();
		// Parse the top-level global function. Everything else is recursive descent
		while (peek()) {
			here = cursor.front();

			if (peekFunctionDefinition()) {
				takeFunctionDefinition();
				continue;
			}

			takeStatement();
		}
		// no endBlock() for global

		// Size of the global stack frame that will persist across ::execute() invocations
		globalFrame = currentFunc->frame;

		callins.clear();
		for (auto& [name,def]: functions) {
			callins[name] = CallIn{def.entry,def.args,def.frame};
		}
	}

	// For std::visit
	struct BoolCaster {
		// Essentially a flag register
		bool b = false;
		void operator()(const bool& bb) { b = bb; }
		void operator()(const I& i) { b = i != 0; }
		void operator()(const F& f) { b = f < 0 || f > 0; }
		void operator()(const S& s) { b = s.size() > 0; }
		void operator()(const U& s) { b = false; }
		void operator()(const Span& s) { b = s.width > 0; }
	};

public:

	struct Result {
		bool ok = true;
		std::string error;
		Value value;

		explicit operator bool() const { return ok; }
		operator std::string() const { return error; }
	};

	class Session {
	private:
		const WESL* wesl = nullptr;

		std::vector<Value> stack;

		struct Frame {
			Size ip = 0; // instruction pointer, offset into code vector
			Size base = 0; // frame base pointer, offset into stack vector
		};

		std::vector<Frame> frames;

		void push(Value v) {
			stack.push_back(v);
		}

		Value pop() {
			Value v = stack.back();
			stack.pop_back();
			return v;
		}

		Value top() {
			return stack.back();
		}

		Frame& frame() {
			return frames.back();
		}

		// A view of the current frame within the data stack
		std::span<Value> locals() {
			return {&stack[frame().base], stack.size()-frame().base};
		}

	public:

		struct Buffer {
		private:
			Session& session;
			Size depth;
		public:
			Buffer(Session& s)
				: session{s}, depth{Size(session.stack.size())}
			{}

			void push(Value v) {
				session.push(v);
			}

			Span span() const {
				return Span{depth, Size(session.stack.size())-depth};
			}

			operator Span() const {
				return span();
			}
		};

		Buffer buffer() {
			return Buffer(*this);
		}

		void clear() {
			wesl = nullptr;
			frames.clear();
			stack.clear();
		}

		void shrink_to_fit() {
			frames.shrink_to_fit();
			stack.shrink_to_fit();
		}

		void reset() {
			frames.clear();
			stack.clear();
			frames.emplace_back();
			stack.resize(wesl->globalFrame);
			while (frame().ip < Size(wesl->code.size())) next();
			assert(frames.size() == 1);
			assert(stack.size() == wesl->globalFrame);
		}

		Session() = default;

		Session(const WESL& w) : wesl{&w} {
			reset();
		}

		size_t memory() const {
			return sizeof(wesl) +
				stack.size() * sizeof(stack.front()) +
				frames.size() * sizeof(frames.front())
			;
		}

		// Extend the current frame (alloca)
		std::span<Value> buffer(Size cells, Value zero, Span& val) {
			val = Span{Size(stack.size()), cells};
			stack.resize(stack.size()+cells, zero);
			return std::span<Value>(stack.data() + val.start, val.width);
		}

	private:
		// Execute the next instruction
		void next() {
			using namespace std;
			auto op = get<Opcode>(wesl->code[frame().ip++]);

			switch (op) {
				case Opcode::Call: {
					Function fn = get<Function>(wesl->code[frame().ip++]);
					frames.emplace_back();
					frame().ip = fn.entry;
					frame().base = stack.size();
					stack.resize(stack.size() + fn.frame);
					break;
				}
				case Opcode::Return: {
					Value v = stack.back();
					stack.resize(frame().base);
					frames.pop_back();
					stack.push_back(v);
					break;
				}
				case Opcode::Jump: {
					frame().ip = get<Size>(wesl->code[frame().ip++]);
					break;
				}
				case Opcode::JZ: {
					Size to = get<Size>(wesl->code[frame().ip++]);
					BoolCaster caster;
					visit(caster,pop());
					if (!caster.b) frame().ip = to;
					break;
				}
				case Opcode::JNZ: {
					Size to = get<Size>(wesl->code[frame().ip++]);
					BoolCaster caster;
					visit(caster,pop());
					if (caster.b) frame().ip = to;
					break;
				}
				case Opcode::CallOut: {
					CallOut* f = get<CallOut*>(wesl->code[frame().ip++]);
					std::array<Value,8> args;
					assert(f->args.size() <= 8);
					for (Size i = 0; i < f->args.size(); i++) {
						args[i] = stack[stack.size()-f->args.size()+i];
					}
					stack.resize(stack.size()-f->args.size());
					push(f->fn(*this,args));
					break;
				}
				case Opcode::LStore: {
					Size o = get<Size>(wesl->code[frame().ip++]);
					locals()[o] = pop();
					break;
				}
				case Opcode::LFetch: {
					Size o = get<Size>(wesl->code[frame().ip++]);
					push(locals()[o]);
					break;
				}
				case Opcode::GStore: {
					Size o = get<Size>(wesl->code[frame().ip++]);
					stack[o] = pop();
					break;
				}
				case Opcode::GFetch: {
					Size o = get<Size>(wesl->code[frame().ip++]);
					push(stack[o]);
					break;
				}
				case Opcode::Print: {
					Value v = pop();
					cout << v;
					break;
				}
				case Opcode::ICast: {
					push(I(get<F>(pop())));
					break;
				}
				case Opcode::FCast: {
					push(F(get<I>(pop())));
					break;
				}
				case Opcode::BCast: {
					BoolCaster caster;
					visit(caster,pop());
					push(caster.b);
					break;
				}
				case Opcode::AddI: {
					I b = get<I>(pop());
					I a = get<I>(pop());
					push(a+b);
					break;
				}
				case Opcode::AddF: {
					F b = get<F>(pop());
					F a = get<F>(pop());
					push(a+b);
					break;
				}
				case Opcode::AddS: {
					S b = get<S>(pop());
					S a = get<S>(pop());
					push(a+b);
					break;
				}
				case Opcode::SubI: {
					I b = get<I>(pop());
					I a = get<I>(pop());
					push(a-b);
					break;
				}
				case Opcode::SubF: {
					F b = get<F>(pop());
					F a = get<F>(pop());
					push(a-b);
					break;
				}
				case Opcode::MulI: {
					I b = get<I>(pop());
					I a = get<I>(pop());
					push(a*b);
					break;
				}
				case Opcode::MulF: {
					F b = get<F>(pop());
					F a = get<F>(pop());
					push(a*b);
					break;
				}
				case Opcode::DivI: {
					I b = get<I>(pop());
					I a = get<I>(pop());
					push(a/b);
					break;
				}
				case Opcode::DivF: {
					F b = get<F>(pop());
					F a = get<F>(pop());
					push(a/b);
					break;
				}
				case Opcode::ModI: {
					I b = get<I>(pop());
					I a = get<I>(pop());
					push(a%b);
					break;
				}
				case Opcode::Lt: {
					Value b = pop();
					Value a = pop();
					push(a<b);
					break;
				}
				case Opcode::Gt: {
					Value b = pop();
					Value a = pop();
					push(a>b);
					break;
				}
				case Opcode::Lte: {
					Value b = pop();
					Value a = pop();
					push(a<=b);
					break;
				}
				case Opcode::Gte: {
					Value b = pop();
					Value a = pop();
					push(a>=b);
					break;
				}
				case Opcode::Eq: {
					Value b = pop();
					Value a = pop();
					push(a==b);
					break;
				}
				case Opcode::NEq: {
					Value b = pop();
					Value a = pop();
					push(!(a==b));
					break;
				}
				case Opcode::Literal: {
					auto v = wesl->code[frame().ip++];
					struct Pusher {
						Session& session;
						Pusher(Session& s) : session{s} {}
						void operator()(const Opcode& v) { throw; }
						void operator()(const Size& v) { throw; }
						void operator()(const bool& v) { session.push(v); }
						void operator()(const I& v) { session.push(v); }
						void operator()(const S& v) { session.push(v); }
						void operator()(const F& v) { session.push(v); }
						void operator()(const Function& v) { throw; }
						void operator()(const CallOut* v) { throw; }
					} pusher(*this);
					visit(pusher, v);
					break;
				}
				case Opcode::Drop: {
					pop();
					break;
				}
				case Opcode::LSpan: {
					Size off = get<Size>(wesl->code[frame().ip++]);
					Size len = get<Size>(wesl->code[frame().ip++]);
					push(Span{frame().base+off,len});
					break;
				}
				case Opcode::GSpan: {
					Size off = get<Size>(wesl->code[frame().ip++]);
					Size len = get<Size>(wesl->code[frame().ip++]);
					push(Span{off,len});
					break;
				}
				case Opcode::Get: {
					Size off = max(I(0), get<I>(pop()));
					Span spn = get<Span>(pop());
					Value v(I(0));
					if (spn.width > 0) {
						off = min(spn.width-1, off);
						if (spn.start + off < stack.size()) {
							v = stack[spn.start + off];
						}
					}
					push(v);
					break;
				}
				case Opcode::Set: {
					Value v = pop();
					Size off = max(I(0), get<I>(pop()));
					Span spn = get<Span>(pop());
					if (spn.width > 0) {
						off = min(spn.width-1, off);
						if (spn.start + off < stack.size()) {
							stack[spn.start + off] = v;
						}
					}
					break;
				}
				case Opcode::SizeOf: {
					Span spn = get<Span>(pop());
					push(spn.width);
					break;
				}
				case Opcode::Times: {
					Size loc = get<Size>(wesl->code[frame().ip++]);
					Size done = get<Size>(wesl->code[frame().ip++]);
					I index = get<I>(pop());
					I limit = get<I>(top());
					push(++index);
					if (index == limit) {
						frame().ip = done;
						break;
					}
					locals()[loc] = index;
					break;
				}
				case Opcode::Range: {
					Size loc = get<Size>(wesl->code[frame().ip++]);
					Size done = get<Size>(wesl->code[frame().ip++]);
					Span spn = get<Span>(pop());
					if (spn.width == 0) {
						push(spn); // balance stack
						frame().ip = done;
						break;
					}
					push(Span{spn.start+1,spn.width-1});
					locals()[loc] = stack[spn.start];
					break;
				}
			}
		}

	public:

		bool running() const {
			return frames.size() > 1;
		}

		Result enter(const std::string& name, std::span<Value> args) {
			Result result;

			if (running()) {
				result.ok = false;
				result.error = "session already running";
				return result;
			}

			assert(frames.size() == 1);
			assert(stack.size() == wesl->globalFrame);

			auto it = wesl->callins.find(name);

			if (it == wesl->callins.end()) {
				result.ok = false;
				result.error = "function " + name + " unknown";
				return result;
			}

			auto& func = it->second;

			if (args.size() != func.args.size()) {
				result.ok = false;
				result.error = "function " + name + " requires " + std::to_string(int(func.args.size())) + " arguments";
				return result;
			}

			frames.emplace_back();
			frame().ip = func.entry;
			frame().base = stack.size();
			stack.insert(stack.end(), args.begin(), args.end());
			stack.resize(stack.size() + func.frame - args.size());

			assert(running());

			return result;
		}

		Result advance() {
			Result result;
			if (running()) {
				try {
					next();
				}
				catch(std::exception& e) {
					result = {false,e.what(),Value()};
				}
			}
			else {
				result = {false,"not running",Value()};
			}
			return result;
		}

		void halt() {
			while (running()) {
				stack.resize(frames.back().base);
				frames.pop_back();
			}
		}

		Result call(const std::string& name, std::span<Value> args, int maxops = 0) {
			Result result = enter(name, args);

			for (int i = 0; running() && result.ok && (maxops == 0 || i < maxops); i++) {
				result = advance();
			}

			if (!running() && result.ok) {
				result.value = stack.back();
				stack.pop_back();
			}

			if (running() && result.ok) {
				result = {false,"exceeded operations",Value()};
			}

			halt();

			assert(frames.size() == 1);
			assert(stack.size() == wesl->globalFrame);

			return result;
		}

		typedef std::function<std::string(const I&)> ISerialize;
		typedef std::function<I(const std::string&)> IUnserialize;

		typedef std::function<std::string(const F&)> FSerialize;
		typedef std::function<F(const std::string&)> FUnserialize;

		typedef std::function<std::string(const S&)> SSerialize;
		typedef std::function<S(const std::string&)> SUnserialize;

		typedef std::function<std::string(const U&)> USerialize;
		typedef std::function<U(const std::string&)> UUnserialize;

		// Export VM state as text. This is only the runtime stacks and instruction pointer, and not
		// the byte code and call tables. This can only be unserialized into a fresh VM that has
		// compiled the same source code.
		std::string serialize(
			ISerialize iSerialize,
			FSerialize fSerialize,
			SSerialize sSerialize,
			USerialize uSerialize
		) {
			using namespace std;
			ostringstream state;

			state << "stack " << stack.size() << ' ';
			for (auto& value: stack) {
				if (holds_alternative<bool>(value)) state << "bool " << (get<bool>(value) ? "true": "false") << ' ';
				if (holds_alternative<I>(value)) { string s = iSerialize(get<I>(value)); state << "I " << s.size() << ' ' << s << ' '; }
				if (holds_alternative<F>(value)) { string s = fSerialize(get<F>(value)); state << "F " << s.size() << ' ' << s << ' '; }
				if (holds_alternative<S>(value)) { string s = sSerialize(get<S>(value)); state << "S " << s.size() << ' ' << s << ' '; }
				if (holds_alternative<U>(value)) { string s = uSerialize(get<U>(value)); state << "U " << s.size() << ' ' << s << ' '; }
				if (holds_alternative<Span>(value)) state << "Span " << get<Span>(value).start << ' ' << get<Span>(value).width << ' ';
			}

			state << "frames " << frames.size() << ' ';
			for (auto& frame: frames) state << frame.ip << ' ' << frame.base << ' ';

			return state.str();
		}

		// Import VM state from text. The VM must have compiled the same source code and
		// attached the same callback functions that were in place when this text was
		// serialized.
		void unserialize(const std::string& s,
			IUnserialize iUnserialize,
			FUnserialize fUnserialize,
			SUnserialize sUnserialize,
			UUnserialize uUnserialize
		) {
			using namespace std;
			istringstream state(s);

			auto expect = [&](const string& word) {
				string in; state >> in;
				if (in != word) throw runtime_error("expected keyword " + word);
			};

			Size depth;

			expect("stack");
			state >> depth;
			stack.resize(depth);

			for (Size i = 0; i < depth; i++) {
				string type;
				state >> type;

				Size size;
				state >> size;

				char space;
				state.read(&space, 1);

				vector<char> buf(size+1, 0);
				state.read(buf.data(), size);
				string value(buf.data());

				if (type == "bool") stack[i] = (value == "true");
				else if (type == "I") stack[i] = iUnserialize(value);
				else if (type == "F") stack[i] = fUnserialize(value);
				else if (type == "S") stack[i] = sUnserialize(value);
				else if (type == "U") stack[i] = uUnserialize(value);
				else if (type == "Span") {
					Span span;
					state >> span.start;
					state >> span.width;
					stack[i] = span;
				}
				else {
					throw runtime_error("bad type: " + type);
				}
			}

			expect("frames");
			state >> depth;
			frames.resize(depth);
			for (auto& frame: frames) {
				state >> frame.ip;
				state >> frame.base;
			}
		}
	};

	void clear() {
		code.clear();
	}

	Result compile(std::string_view src) {
		clear();
		try {
			processSourceCode(src);
		}
		catch(std::exception& e) {
			return {false,e.what(),Value()};
		}
		return {true,"",Value()};
	}

	Session session() const {
		return Session(*this);
	}
};
