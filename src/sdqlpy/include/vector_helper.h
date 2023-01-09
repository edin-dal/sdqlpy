namespace std {

	template<typename T>
	std::ostream& operator<<(std::ostream& out, const vector<T>& v) {

		size_t counter = 0;

		out << fixed << setprecision(2) << boolalpha;
		out << "[ ";
		for (auto& item : v)
		{
			out << item;
			counter++;
			if (counter != v.size())
				out << ", ";
		}
		out << " ]";

		return out;
	}
}


template <typename T1, typename T2>
inline std::vector<T1, T2>& operator += (std::vector<T1, T2>& lhs, const std::vector<T1, T2>& rhs)
{
	// lhs.reserve(lhs.size() + rhs.size());
	// lhs.insert(lhs.end(), rhs.begin(), rhs.end());
	std::move(rhs.begin(), rhs.end(), std::back_inserter(lhs));
	return lhs;
}

template <typename T1, typename T2>
std::vector<T1, T2> operator + (std::vector<T1, T2>& lhs, const std::vector<T1, T2>& rhs)
{
	vector<T1, T2> res;
	for (auto& i : rhs)
		res.emplace_back(i);
	for (auto& i : lhs)
		res.emplace_back(i);
	return res;
}

template <typename T1, typename T2>
std::vector<T1, T2> operator + (const std::vector<T1, T2>& lhs, std::vector<T1, T2>& rhs)
{
	vector<T1, T2> res;
	for (auto& i : rhs)
		res.emplace_back(i);
	for (auto& i : lhs)
		res.emplace_back(i);
	return res;
}