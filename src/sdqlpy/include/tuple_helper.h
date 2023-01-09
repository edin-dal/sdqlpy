namespace std {

	namespace SDQLInternal
	{
		template <typename Tuple, typename F, std::size_t ...Indices>
		void for_each_impl(Tuple&& tuple, F&& f, std::index_sequence<Indices...>) {
			using swallow = int[];
			(void)swallow {
				1,
					(f(std::get<Indices>(std::forward<Tuple>(tuple))), void(), int{})...
			};
		}

		template <typename Tuple, typename F>
		void for_each(Tuple&& tuple, F&& f) {
			constexpr std::size_t N = std::tuple_size<std::remove_reference_t<Tuple>>::value;
			for_each_impl(std::forward<Tuple>(tuple), std::forward<F>(f),
				std::make_index_sequence<N>{});
		}

		template<typename T, size_t... Is>
		void printTuple(std::ostream& out, const T& v)
		{
			int counter = 0;
			out << "<";
			for_each(v, [&](auto x) 
			{
				out << x;
				counter++;
				if (counter != tuple_size<T>{})
					out << ",";
			});
			out << ">";
		}

		template<typename T, size_t... Is>
		inline void tuple_add_rhs_to_lhs(T& t1, const T& t2, std::integer_sequence<size_t, Is...>)
		{
			auto l = { (std::get<Is>(t1) += std::get<Is>(t2), 0)... };
			(void)l; // prevent unused warning
		}

		template <class T>
        inline void hash_combine(std::size_t& seed, T const& v)
        {
            seed ^= hash<T>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
        }

        // Recursive template code derived from Matthieu M.
        template <class Tuple, size_t Index = std::tuple_size<Tuple>::value - 1>
        struct HashValueImpl
        {
            static void apply(size_t& seed, Tuple const& tuple)
            {
                HashValueImpl<Tuple, Index - 1>::apply(seed, tuple);
                hash_combine(seed, get<Index>(tuple));
            }
        };

        template <class Tuple>
        struct HashValueImpl<Tuple, 0>
        {
            static void apply(size_t& seed, Tuple const& tuple)
            {
                hash_combine(seed, get<0>(tuple));
            }
        };

	}


	template<typename...T>
	std::ostream& operator<<(std::ostream& out, const std::tuple<T...>& v) {
		SDQLInternal::printTuple(out, v);
		return out;
	}


	template <typename...T>
	inline std::tuple<T...>& operator += (std::tuple<T...>& lhs, const std::tuple<T...>& rhs)
	{
		SDQLInternal::tuple_add_rhs_to_lhs(lhs, rhs, std::index_sequence_for<T...>{});
		return lhs;
	}

	template <typename...T>
	inline std::tuple<T...> operator + (std::tuple<T...> lhs, const std::tuple<T...>& rhs)
	{
		return lhs += rhs;
	}


    template <typename ... TT>
    struct hash<std::tuple<TT...>>
    {
        size_t operator()(std::tuple<TT...> const& tt) const
        {
            size_t seed = 0;
            SDQLInternal::HashValueImpl<std::tuple<TT...> >::apply(seed, tt);
            return seed;
        }

    };
}