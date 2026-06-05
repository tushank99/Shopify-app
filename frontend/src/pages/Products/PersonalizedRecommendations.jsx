import { useGetPersonalizedRecommendationsQuery } from "../../redux/api/productApiSlice"; 
import { FaStar } from "react-icons/fa"; 
import Loader from "../../components/Loader";
import ProductCard from "./ProductCard"; 

const PersonalizedRecommendations = () => {
  const { data: products, isLoading, error } = useGetPersonalizedRecommendationsQuery();

  if (isLoading) {
    return (
      <div className="flex justify-center py-10 bg-gray-900">
        <Loader />
      </div>
    );
  }

  if (error || !products || products.length === 0) {
    return null;
  }

  return (
    <section className="py-12 bg-gray-800 border-b border-gray-700">
      <div className="max-w-7xl mx-auto px-6">
        <div className="mb-8">
          <h2 className="text-2xl lg:text-3xl font-bold text-white flex items-center gap-3">
            <FaStar className="text-yellow-400 animate-pulse text-xl lg:text-2xl" /> 
            Recommended For You
          </h2>
          <p className="text-gray-400 mt-1">Personalized using your browsing history and ratings</p>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
          {products.slice(0, 4).map((product) => (
            <ProductCard key={product._id} product={product} />
          ))}
        </div>
      </div>
    </section>
  );
};

export default PersonalizedRecommendations;